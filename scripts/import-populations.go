package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

type row struct {
	fips   string
	abbrev string
}

func importPopulations() {
	ctx := context.Background()

	f, err := os.Open("./data/states-populations.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	db, err := createDBClient(ctx)
	if err != nil {
		panic(err)
	}

	api := db.Collection("states-api")

	reader := csv.NewReader(f)
	reader.Comma = ','

	firstLine := true
	abbrevs := make(map[string]string)
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
		row := processRow(data)
		abbrevs[row.fips] = row.abbrev
	}

	iter := api.Documents(ctx)
	defer iter.Stop()
	wg := sync.WaitGroup{}
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}

		wg.Add(1)
		go func(doc *firestore.DocumentSnapshot) {
			f := doc.Data()["Fips"]
			fips := f.(string)

			fmt.Print(".")
			doc.Ref.Set(ctx, map[string]interface{}{
				"Abbreviation": abbrevs[fips],
			}, firestore.MergeAll)

			wg.Done()
		}(doc)
	}

	wg.Wait()
}

func processRow(data []string) row {
	return row{
		fips:   data[1],
		abbrev: data[0],
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
