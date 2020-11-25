package p

import (
	"context"
	"encoding/csv"
	"io"
	"net/http"
	"sync"

	"cloud.google.com/go/firestore"
)

type row struct {
	Date            string
	County          string
	State           string
	Fips            string
	Cases           string
	Deaths          string
	ConfirmedCases  string
	ConfirmedDeaths string
	ProbableCases   string
	ProbableDeaths  string
}

type processor func([]string) row

// ImportLiveCounties imports live data for every county in the US
func ImportLiveCounties() {
	importLive("counties-live", "https://raw.githubusercontent.com/nytimes/covid-19-data/master/live/us-counties.csv", processCountyRow)
}

// ImportLiveStates imports live data for every state in the US
func ImportLiveStates() {
	importLive("states-live", "https://raw.githubusercontent.com/nytimes/covid-19-data/master/live/us-states.csv", processStateRow)
}

func importLive(collectionName string, url string, processRow processor) {
	ctx := context.Background()
	db, err := createDBClient(ctx)
	if err != nil {
		panic(err)
	}

	resp, err := http.Get(url)
	if err != nil {
		panic(err)
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

		if firstLine {
			firstLine = false
			continue
		}

		wg.Add(1)
		go func(d []string, w *sync.WaitGroup) {
			row := processRow(d)

			if len(row.Fips) == 0 {
				wg.Done()
				return
			}

			collection := db.Collection(collectionName)
			if _, err := collection.Doc(row.Fips).Set(ctx, row); err != nil {
				panic(err)
			}
			w.Done()
		}(data, &wg)
	}

	wg.Wait()
}

func processStateRow(r []string) row {
	return row{
		Date:            r[0],
		State:           r[1],
		Fips:            r[2],
		Cases:           r[3],
		Deaths:          r[4],
		ConfirmedCases:  r[5],
		ConfirmedDeaths: r[6],
		ProbableCases:   r[7],
		ProbableDeaths:  r[8],
	}
}

func processCountyRow(r []string) row {
	return row{
		Date:            r[0],
		County:          r[1],
		State:           r[2],
		Fips:            r[3],
		Cases:           r[4],
		Deaths:          r[5],
		ConfirmedCases:  r[6],
		ConfirmedDeaths: r[7],
		ProbableCases:   r[8],
		ProbableDeaths:  r[9],
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
