package p

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"

	"cloud.google.com/go/firestore"
)

func ImportDaily() {

}

func getPreviousOffset(ctx context.Context, db *firestore.Client, scope string, url string) int64 {
	// fetch previous stopping point
	doc, err := db.Collection("offsets").Doc(scope).Get(ctx)
	if err != nil {
		panic(err)
	}

	o, err := doc.DataAt("offset")
	if err != nil {
		panic(err)
	}

	offset := o.(int64)

	return offset
}

func getCurrentOffset(ctx context.Context, url string) int {
	h, err := http.Head(url)
	if err != nil {
		panic(err)
	}
	defer h.Body.Close()

	clen := h.Header["Content-Length"][0]
	len, err := strconv.Atoi(clen)
	if err != nil {
		panic(err)
	}

	return len - 1
}

func ImportHistorical(scope string, collectionName string, url string) {
	ctx := context.Background()

	db, err := createDBClient(ctx)
	if err != nil {
		panic(err)
	}

	previousOffset := getPreviousOffset(ctx, db, scope, url)
	// currentOffset := getCurrentOffset(ctx, url)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-", previousOffset))

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 416 {
		log.Println("No new data in file.")
		return
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
			panic(err)
		}

		if firstLine && previousOffset == 0 {
			firstLine = false
			continue
		}

		wg.Add(1)
		go func(d []string, w *sync.WaitGroup) {
			row := processRow(d)

			fmt.Print(".")
			// log.Println("processing row", row.Fips+"_"+row.Date)
			collection := db.Collection(collectionName)
			if _, err := collection.Doc(row.Fips+"_"+row.Date).Set(ctx, row); err != nil {
				panic(err)
			}
			w.Done()
		}(data, &wg)
	}

	length := resp.Header["Content-Length"]

	_, err = db.Collection("offsets").Doc(scope).Update(ctx, []firestore.Update{{
		Path:  "offset",
		Value: length,
	}})

	if err != nil {
		panic(err)
	}

	wg.Wait()
}

func ImportHistoricalState() {
	ImportHistorical("state", "states-historical", "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv")
}

func ImportHistoricalCounty() {
	ImportHistorical("county", "counties-historical", "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv")
}

type stateHistorical struct {
	Date   string
	County string
	State  string
	Fips   string
	Cases  string
	Deaths string
}

func processRow(row []string) stateHistorical {
	return stateHistorical{
		Date:   row[0],
		County: row[1],
		State:  row[2],
		Fips:   row[3],
		Cases:  row[4],
		Deaths: row[5],
	}
}

func createDBClient(ctx context.Context) (*firestore.Client, error) {
	projectID := "covid-near-me-296621"

	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	return client, nil
}
