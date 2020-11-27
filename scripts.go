package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

type row struct {
	fips       string
	population int
}

func importStatePopulations() {
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
	populations := make(map[string]int)
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
		row := processStateRow(data)
		populations[row.fips] = row.population
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
				"Population": populations[fips],
			}, firestore.MergeAll)

			wg.Done()
		}(doc)
	}

	wg.Wait()
}

func processStateRow(data []string) row {
	population, _ := strconv.Atoi(data[3])

	return row{
		fips:       data[1],
		population: population,
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

func importCountyPopulations() {
	ctx := context.Background()

	f, err := os.Open("./data/counties-populations.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	db, err := createDBClient(ctx)
	if err != nil {
		panic(err)
	}

	api := db.Collection("counties-api")

	reader := csv.NewReader(f)
	reader.Comma = ','

	firstLine := true
	populations := make(map[string]int)
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
		row := processCountyRow(data)
		populations[row.fips] = row.population
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
				"Population": populations[fips],
			}, firestore.MergeAll)

			wg.Done()
		}(doc)
	}

	wg.Wait()
}

func processCountyRow(data []string) row {
	population, _ := strconv.Atoi(data[2])

	return row{
		fips:       data[3],
		population: population,
	}
}
