package p

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

const (
	layoutISO = "2006-01-02"
)

type Row struct {
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

func StoreActiveCasesForState(ctx context.Context, message interface{}) error {
	return storeActiveCases(ctx, "states")
}

func StoreActiveCasesForCounty(ctx context.Context, message interface{}) error {
	return storeActiveCases(ctx, "counties")
}

func storeActiveCases(ctx context.Context, collectionPrefix string) error {
	db, err := createDBClient(ctx)
	if err != nil {
		return err
	}

	iter := db.Collection(collectionPrefix + "-live").Documents(ctx)
	defer iter.Stop()
	wg := sync.WaitGroup{}

	for {
		doc, err := iter.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			return err
		}

		var row Row
		doc.DataTo(&row)

		wg.Add(1)
		go func(row Row) {
			if err := calculateActiveCases(ctx, collectionPrefix, row); err != nil {
				log.Println("failed", row.Fips)
				log.Println(err)
			}
			wg.Done()
		}(row)
	}

	wg.Wait()
	return nil
}

func calculateActiveCases(ctx context.Context, collectionPrefix string, row Row) error {
	db, err := createDBClient(ctx)
	if err != nil {
		panic(err)
	}

	fips := row.Fips
	historical := db.Collection(collectionPrefix + "-historical")
	live := db.Collection(collectionPrefix + "-live").Doc(fips)
	api := db.Collection(collectionPrefix + "-api").Doc(fips)

	currentCases, deaths, err := getLiveNumbers(ctx, fips, live)
	if err != nil {
		return err
	}
	daysAgo14Cases, err := getCasesFromDaysAgo(ctx, fips, 14, "Cases", historical)
	if err != nil {
		return err
	}
	daysAgo1Cases, err := getCasesFromDaysAgo(ctx, fips, 1, "Cases", historical)
	if err != nil {
		return err
	}
	daysAgo1Deaths, err := getCasesFromDaysAgo(ctx, fips, 1, "Deaths", historical)
	if err != nil {
		return err
	}
	daysAgo15Cases, err := getCasesFromDaysAgo(ctx, fips, 15, "Cases", historical)
	if err != nil {
		return err
	}
	daysAgo25Cases, err := getCasesFromDaysAgo(ctx, fips, 25, "Cases", historical)
	if err != nil {
		return err
	}
	daysAgo26Cases, err := getCasesFromDaysAgo(ctx, fips, 26, "Cases", historical)
	if err != nil {
		return err
	}
	daysAgo49Cases, err := getCasesFromDaysAgo(ctx, fips, 49, "Cases", historical)
	if err != nil {
		return err
	}

	activeCaseCount := computeActiveCaseCount(currentCases,
		daysAgo14Cases,
		daysAgo15Cases,
		daysAgo25Cases,
		daysAgo26Cases,
		daysAgo49Cases,
		deaths)

	if _, err = api.Set(ctx, map[string]interface{}{
		"Date":            row.Date,
		"County":          row.County,
		"State":           row.State,
		"Fips":            row.Fips,
		"Cases":           row.Cases,
		"Deaths":          row.Deaths,
		"ConfirmedCases":  row.ConfirmedCases,
		"ConfirmedDeaths": row.ConfirmedDeaths,
		"ProbableCases":   row.ProbableCases,
		"ProbableDeaths":  row.ProbableDeaths,
		// Calculated fields
		"ActiveCases":    activeCaseCount,
		"NewCasesToday":  currentCases - daysAgo1Cases,
		"NewDeathsToday": deaths - daysAgo1Deaths,
	}, firestore.MergeAll); err != nil {
		panic(err)
	}

	return nil
}

func computeActiveCaseCount(current, days14, days15, days25, days26, days49, deaths int) int {
	return int(
		float32(current-days14) + (0.19 * float32(days15-days25)) + (0.05 * float32(days26-days49)) - float32(deaths),
	)
}

func getCasesFromDaysAgo(ctx context.Context, fips string, daysAgo int, fieldName string, cases *firestore.CollectionRef) (int, error) {
	today := time.Now()
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		panic(err)
	}

	fmt.Println("time.Now", time.Now())
	fmt.Println("time.UTC", time.Now().UTC())

	today = today.In(location)
	fmt.Println("time.EST", today)

	date := today.AddDate(0, 0, -daysAgo).Format(layoutISO)

	docsnap, err := cases.Doc(fips + "_" + date).Get(ctx)
	if err != nil {
		return 0, err
	}

	return fetchNumericFieldFromDoc(docsnap, fieldName)
}

func fetchNumericFieldFromDoc(doc *firestore.DocumentSnapshot, fieldName string) (int, error) {
	data, err := doc.DataAt(fieldName)

	if err != nil {
		panic(err)
	}

	cast := data.(string)

	i, err := strconv.Atoi(cast)

	if err != nil {
		return 0, err
	}

	return i, nil
}

func getLiveNumbers(ctx context.Context, fips string, doc *firestore.DocumentRef) (int, int, error) {
	docsnap, err := doc.Get(ctx)
	if err != nil {
		panic(err)
	}

	rawCases, err := fetchNumericFieldFromDoc(docsnap, "Cases")
	rawDeaths, err := fetchNumericFieldFromDoc(docsnap, "Deaths")

	if err != nil {
		return 0, 0, err
	}

	return rawCases, rawDeaths, nil
}

func createDBClient(ctx context.Context) (*firestore.Client, error) {
	projectID := "covid-near-me-296621"

	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	return client, nil
}
