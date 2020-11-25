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

func ImportHistorical() {
	url := "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv"
	ctx := context.Background()

	db, err := createDBClient(ctx)
	if err != nil {
		panic(err)
	}

	// fetch previous stopping point
	docsnap, err := db.Collection("offsets").Doc("state").Get(ctx)
	if err != nil {
		panic(err)
	}

	o, err := docsnap.DataAt("offset")
	if err != nil {
		panic(err)
	}

	offset := o.(int)

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

	client := &http.Client{}
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, len-1))

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

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

		if firstLine {
			firstLine = false
			continue
		}

		wg.Add(1)
		go func(d []string, w *sync.WaitGroup) {
			row := processRow(d)

			log.Println("processing row", row.Date)
			collection := db.Collection("states-historical")
			if _, err := collection.Doc(row.Fips+"_"+row.Date).Set(ctx, row); err != nil {
				panic(err)
			}
			w.Done()
		}(data, &wg)
	}

	length := resp.Header["Content-Length"]
	db.Collection("offsets").Doc().Set(ctx)

	wg.Wait()
}

type stateHistorical struct {
	Date   string
	State  string
	Fips   string
	Cases  string
	Deaths string
}

func processRow(row []string) stateHistorical {
	return stateHistorical{
		Date:   row[0],
		State:  row[1],
		Fips:   row[2],
		Cases:  row[3],
		Deaths: row[4],
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
