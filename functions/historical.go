package functions

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/getsentry/sentry-go"
)

// ImportStatesHistorical Cloud Function
func ImportStatesHistorical(ctx context.Context, message interface{}) error {
	return importHistorical("state", "states-historical", "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv", processHistoricalStateRow)
}

// ImportCountiesHistorical Cloud Function
func ImportCountiesHistorical(ctx context.Context, message interface{}) error {
	return importHistorical("county", "counties-historical", "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv", processHistoricalCountyRow)
}

func importHistorical(scope string, collectionName string, url string, processRow func([]string) historicalRow) error {
	ctx := context.Background()
	err := sentry.Init(sentry.ClientOptions{
		Dsn: os.Getenv("SENTRY_DSN"),
	})
	if err != nil {
		panic(err)
	}
	defer sentry.Flush(2 * time.Second)

	db, err := createDBClient(ctx)
	if err != nil {
		sentry.CaptureException(err)
		panic(err)
	}

	previousOffset := getPreviousOffset(ctx, db, scope, url)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-", previousOffset))

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("http request failed (likely a connectivity problem)", collectionName)
		sentry.CaptureException(err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 416 {
		fmt.Println("No new data in file", collectionName)
		return nil
	}

	reader := csv.NewReader(resp.Body)
	reader.Comma = ','

	firstLine := true

	wg := sync.WaitGroup{}

	batch := db.Batch()
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("error reading CSV line")
			sentry.CaptureException(err)
			return err
		}

		if firstLine && previousOffset == 0 {
			firstLine = false
			continue
		}

		wg.Add(1)
		go func(d []string, w *sync.WaitGroup) {
			row := processRow(d)

			if row.County == "New York City" {
				row.Fips = "NYC"
			}

			doc := db.Collection(collectionName).Doc(row.Fips + "_" + row.Date)

			batch.Set(doc, row)
			w.Done()
		}(data, &wg)
	}

	length := getCurrentOffset(ctx, url)

	_, err = db.Collection("offsets").Doc(scope).Update(ctx, []firestore.Update{{
		Path:  "offset",
		Value: length,
	}})
	if err != nil {
		fmt.Println("failed to update offset", scope)
		sentry.CaptureException(err)
		return nil
	}

	wg.Wait()

	if _, err := batch.Commit(ctx); err != nil {
		sentry.CaptureException(err)
		fmt.Println("failed to write all documents")
	}
	return nil
}

func getPreviousOffset(ctx context.Context, db *firestore.Client, scope string, url string) int64 {
	// fetch previous stopping point
	doc, err := db.Collection("offsets").Doc(scope).Get(ctx)
	if err != nil {
		fmt.Println("failed to fetch offsets document")
		sentry.CaptureException(err)
		panic(err)
	}

	o, err := doc.DataAt("offset")
	if err != nil {
		fmt.Println("failed to fetch offset value from document")
		sentry.CaptureException(err)
		panic(err)
	}

	offset := o.(int64)

	fmt.Println("offset", offset)
	return offset
}

func getCurrentOffset(ctx context.Context, url string) int {
	h, err := http.Head(url)
	if err != nil {
		sentry.CaptureException(err)
		panic(err)
	}
	defer h.Body.Close()

	clen := h.Header["Content-Length"][0]
	len, err := strconv.Atoi(clen)
	if err != nil {
		sentry.CaptureException(err)
		panic(err)
	}

	return len
}

func processHistoricalStateRow(row []string) historicalRow {
	return historicalRow{
		Date:   row[0],
		State:  row[1],
		Fips:   row[2],
		Cases:  row[3],
		Deaths: row[4],
	}
}

func processHistoricalCountyRow(row []string) historicalRow {
	return historicalRow{
		Date:   row[0],
		County: row[1],
		State:  row[2],
		Fips:   row[3],
		Cases:  row[4],
		Deaths: row[5],
	}
}
