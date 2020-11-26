package functions

import (
	"context"

	"cloud.google.com/go/firestore"
	"github.com/getsentry/sentry-go"
)

// historicalRow represents a row from one of the historical datasets
type historicalRow struct {
	Date   string
	County string
	State  string
	Fips   string
	Cases  string
	Deaths string
}

// liveRow represents a row from one of the live datasets
// https://github.com/nytimes/covid-19-data/tree/master/live
type liveRow struct {
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

// computedRow represents an entry in the states or counties api collection
type computedRow struct {
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
	ActiveCases     int
	NewCasesToday   int
	NewDeathsToday  int
}

func createDBClient(ctx context.Context) (*firestore.Client, error) {
	projectID := "covid-near-me-296621"

	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		sentry.CaptureException(err)
		return nil, err
	}

	return client, nil
}
