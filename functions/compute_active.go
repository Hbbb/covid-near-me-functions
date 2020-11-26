package functions

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/getsentry/sentry-go"
	"google.golang.org/api/iterator"
)

const (
	layoutISO = "2006-01-02"
)

// StoreActiveCasesForState Cloud Function
func StoreActiveCasesForState(ctx context.Context, message interface{}) error {
	fmt.Println("StoreActiveCasesForState()")
	return storeActiveCases(ctx, "states")
}

// StoreActiveCasesForCounty Cloud Function
func StoreActiveCasesForCounty(ctx context.Context, message interface{}) error {
	fmt.Println("StoreActiveCasesForCounty()")
	return storeActiveCases(ctx, "counties")
}

func storeActiveCases(ctx context.Context, collectionPrefix string) error {
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
		return err
	}

	cName := fmt.Sprintf("%s-live", collectionPrefix)

	iter := db.Collection(cName).Documents(ctx)
	defer iter.Stop()
	wg := sync.WaitGroup{}

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			sentry.CaptureException(err)
			return err
		}

		var row computedRow
		doc.DataTo(&row)

		wg.Add(1)

		fmt.Println("processing", row.State, row.County)
		go func(row computedRow) {
			if err := calculateActiveCases(ctx, collectionPrefix, row); err != nil {
				fmt.Println("failed to calculate active cases for", row.State, row.County)
				fmt.Println(err)
				sentry.CaptureException(err)
			}
			wg.Done()
		}(row)
	}
	wg.Wait()

	return nil
}

func calculateActiveCases(ctx context.Context, collectionPrefix string, row computedRow) error {
	db, err := createDBClient(ctx)
	if err != nil {
		sentry.CaptureException(err)
		panic(err)
	}

	fips := row.Fips
	historical := db.Collection(collectionPrefix + "-historical")
	live := db.Collection(collectionPrefix + "-live").Doc(fips)
	api := db.Collection(collectionPrefix + "-api").Doc(fips)

	currentCases, deaths, err := getLiveNumbers(ctx, fips, live)
	if err != nil {
		sentry.CaptureException(err)
		return err
	}
	daysAgo14Cases, err := getCasesFromDaysAgo(ctx, fips, 14, "Cases", historical)
	if err != nil {
		sentry.CaptureException(err)
		return err
	}
	daysAgo1Cases, err := getCasesFromDaysAgo(ctx, fips, 1, "Cases", historical)
	if err != nil {
		sentry.CaptureException(err)
		return err
	}
	daysAgo1Deaths, err := getCasesFromDaysAgo(ctx, fips, 1, "Deaths", historical)
	if err != nil {
		sentry.CaptureException(err)
		return err
	}
	daysAgo15Cases, err := getCasesFromDaysAgo(ctx, fips, 15, "Cases", historical)
	if err != nil {
		sentry.CaptureException(err)
		return err
	}
	daysAgo25Cases, err := getCasesFromDaysAgo(ctx, fips, 25, "Cases", historical)
	if err != nil {
		sentry.CaptureException(err)
		return err
	}
	daysAgo26Cases, err := getCasesFromDaysAgo(ctx, fips, 26, "Cases", historical)
	if err != nil {
		sentry.CaptureException(err)
		return err
	}
	daysAgo49Cases, err := getCasesFromDaysAgo(ctx, fips, 49, "Cases", historical)
	if err != nil {
		sentry.CaptureException(err)
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
		sentry.CaptureException(err)
		panic(err)
	}

	return nil
}

// active case algorithm, taken from:
// https://www.esri.com/arcgis-blog/products/js-api-arcgis/mapping/animate-and-explore-covid-19-data-through-time/#active
func computeActiveCaseCount(current, days14, days15, days25, days26, days49, deaths int) int {
	return int(
		float32(current-days14) + (0.19 * float32(days15-days25)) + (0.05 * float32(days26-days49)) - float32(deaths),
	)
}

func getCasesFromDaysAgo(ctx context.Context, fips string, daysAgo int, fieldName string, cases *firestore.CollectionRef) (int, error) {
	today := time.Now()

	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		sentry.CaptureException(err)
		panic(err)
	}

	today = today.In(location)

	date := today.AddDate(0, 0, -daysAgo).Format(layoutISO)

	docsnap, err := cases.Doc(fips + "_" + date).Get(ctx)
	if err != nil {
		fmt.Println("failed to fetch case numbers for", fips+"_"+date)
		sentry.CaptureException(err)
		return 0, err
	}

	return fetchNumericFieldFromDoc(docsnap, fieldName)
}

func fetchNumericFieldFromDoc(doc *firestore.DocumentSnapshot, fieldName string) (int, error) {
	data, err := doc.DataAt(fieldName)
	if err != nil {
		sentry.CaptureException(err)
		panic(err)
	}

	cast := data.(string)

	i, err := strconv.Atoi(cast)
	if err != nil {
		sentry.CaptureException(err)
		return 0, err
	}

	return i, nil
}

func getLiveNumbers(ctx context.Context, fips string, doc *firestore.DocumentRef) (int, int, error) {
	docsnap, err := doc.Get(ctx)
	if err != nil {
		fmt.Println("failed to fetch live numbers", fips)
		sentry.CaptureException(err)
		panic(err)
	}

	rawCases, err := fetchNumericFieldFromDoc(docsnap, "Cases")
	if err != nil {
		fmt.Println("failed to fetch live cases", fips)
		sentry.CaptureException(err)
		return 0, 0, err
	}

	rawDeaths, err := fetchNumericFieldFromDoc(docsnap, "Deaths")
	if err != nil {
		fmt.Println("failed to fetch live deaths", fips)
		sentry.CaptureException(err)
		return 0, 0, err
	}

	return rawCases, rawDeaths, nil
}
