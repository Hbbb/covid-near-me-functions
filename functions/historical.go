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
	err := sentry.Init(sentry.ClientOptions{
		Dsn: os.Getenv("SENTRY_DSN"),
	})
	if err != nil {
		panic(err)
	}
	defer sentry.Recover()
	defer sentry.Flush(2 * time.Second)

	err = importHistorical("state", "states-historical", "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv", processHistoricalStateRow)
	if err != nil {
		panic(err)
	}

	return nil
}

// ImportCountiesHistorical Cloud Function
func ImportCountiesHistorical(ctx context.Context, message interface{}) error {
	err := sentry.Init(sentry.ClientOptions{
		Dsn: os.Getenv("SENTRY_DSN"),
	})
	if err != nil {
		panic(err)
	}
	defer sentry.Recover()
	defer sentry.Flush(2 * time.Second)

	err = importHistorical("county", "counties-historical", "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv", processHistoricalCountyRow)
	if err != nil {
		panic(err)
	}

	return nil
}

func importHistorical(scope string, collectionName string, url string, processRow func([]string) (historicalRow, error)) error {
	ctx := context.Background()

	db, err := createDBClient(ctx)
	if err != nil {
		return err
	}

	previousOffset, err := getPreviousOffset(ctx, db, scope, url)
	if err != nil {
		return fmt.Errorf("failed to fetch previous offset for %s: %e", scope, err)
	}

	client := &http.Client{}
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-", previousOffset))

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 416 {
		fmt.Printf("No new data in %s spreadsheet\n", scope)
		return nil
	}

	reader := csv.NewReader(resp.Body)
	reader.Comma = ','

	firstLine := true
	wg := sync.WaitGroup{}
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading CSV line: %e", err)
		}

		if firstLine && previousOffset == 0 {
			firstLine = false
			continue
		}

		row, err := processRow(data)
		if err != nil {
			return fmt.Errorf("failed to process row: %e", err)
		}

		wg.Add(1)
		go func(hub *sentry.Hub) {
			hub.ConfigureScope(func(scope *sentry.Scope) {
				scope.SetTag("fips", row.Fips)
				scope.SetTag("state", row.State)
				scope.SetTag("county", row.County)
			})

			if row.County == "New York City" {
				row.Fips = "NYC"
			}

			doc := db.Collection(collectionName).Doc(row.Fips + "_" + row.Date)
			if _, err := doc.Set(ctx, map[string]interface{}{
				"Date":   row.Date,
				"County": row.County,
				"State":  row.State,
				"Fips":   row.Fips,
				"Cases":  row.Cases,
				"Deaths": row.Deaths,
			}, firestore.MergeAll); err != nil {
				fmt.Println("failed to store historical cases for", row.Fips, row.State, row.County)
				hub.CaptureException(err)
			}

			wg.Done()
		}(sentry.CurrentHub().Clone())
	}

	// We update the offset to be the size of the full file
	// Next time we import, when the file grows, we'll pull from this offset
	length, err := getNextOffset(ctx, url)
	if err != nil {
		return fmt.Errorf("failed to fetch next offset: %e", err)
	}

	_, err = db.Collection("offsets").Doc(scope).Update(ctx, []firestore.Update{{
		Path:  "offset",
		Value: length,
	}})
	if err != nil {
		return fmt.Errorf("failed to update next offset: %e", err)
	}

	wg.Wait()
	return nil
}

// fetch previous stopping point (file's Content-Length) from last time we imported
func getPreviousOffset(ctx context.Context, db *firestore.Client, scope string, url string) (int64, error) {
	doc, err := db.Collection("offsets").Doc(scope).Get(ctx)
	if err != nil {
		return 0, err
	}

	o, err := doc.DataAt("offset")
	if err != nil {
		return 0, err
	}

	offset, ok := o.(int64)
	if !ok {
		return 0, err
	}

	return offset, nil
}

func getNextOffset(ctx context.Context, url string) (int, error) {
	h, err := http.Head(url)
	if err != nil {
		return 0, err
	}
	defer h.Body.Close()

	clen := h.Header["Content-Length"][0]
	len, err := strconv.Atoi(clen)
	if err != nil {
		return 0, nil
	}

	return len, nil
}

func processHistoricalStateRow(row []string) (historicalRow, error) {
	if len(row) != 6 {
		return historicalRow{}, fmt.Errorf("csv row length mismatch; bad offset")
	}

	return historicalRow{
		Date:   row[0],
		State:  row[1],
		Fips:   row[2],
		Cases:  row[3],
		Deaths: row[4],
	}, nil
}

func processHistoricalCountyRow(row []string) (historicalRow, error) {
	if len(row) != 6 {
		return historicalRow{}, fmt.Errorf("csv row length mismatch; bad offset")
	}

	return historicalRow{
		Date:   row[0],
		County: row[1],
		State:  row[2],
		Fips:   row[3],
		Cases:  row[4],
		Deaths: row[5],
	}, nil
}
