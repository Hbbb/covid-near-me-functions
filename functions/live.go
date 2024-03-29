package functions

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
)

type processor func([]string) liveRow

// ImportLiveCounties Cloud Function
func ImportLiveCounties(ctx context.Context, message interface{}) error {
	return importLive("counties-live", "https://raw.githubusercontent.com/nytimes/covid-19-data/master/live/us-counties.csv", processLiveCountyRow)
}

// ImportLiveStates Cloud Function
func ImportLiveStates(ctx context.Context, message interface{}) error {
	return importLive("states-live", "https://raw.githubusercontent.com/nytimes/covid-19-data/master/live/us-states.csv", processLiveStateRow)
}

func importLive(collectionName string, url string, processRow processor) error {
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

	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("http request failed (likely a connectivity problem)", collectionName)
		sentry.CaptureException(err)
		return err
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
			fmt.Println("error reading CSV line")
			sentry.CaptureException(err)
			return err
		}

		if firstLine {
			firstLine = false
			continue
		}

		wg.Add(1)
		go func(d []string, w *sync.WaitGroup) {
			row := processRow(d)

			if row.County == "New York City" {
				row.Fips = "NYC"
			}

			if len(row.Fips) == 0 {
				wg.Done()
				return
			}

			collection := db.Collection(collectionName)
			if _, err := collection.Doc(row.Fips).Set(ctx, row); err != nil {
				fmt.Println("failed to write document", collectionName)
				sentry.CaptureException(err)
				panic(err)
			}
			w.Done()
		}(data, &wg)
	}

	wg.Wait()
	return nil
}

func processLiveStateRow(r []string) liveRow {
	return liveRow{
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

func processLiveCountyRow(r []string) liveRow {
	return liveRow{
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
