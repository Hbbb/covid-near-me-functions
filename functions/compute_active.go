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
	err := sentry.Init(sentry.ClientOptions{
		Dsn: os.Getenv("SENTRY_DSN"),
	})
	if err != nil {
		panic(err)
	}
	defer sentry.Recover()
	defer sentry.Flush(2 * time.Second)

	err = storeActiveCases(ctx, "states")
	if err != nil {
		panic(err)
	}

	return nil
}

// StoreActiveCasesForCounty Cloud Function
func StoreActiveCasesForCounty(ctx context.Context, message interface{}) error {
	err := sentry.Init(sentry.ClientOptions{
		Dsn: os.Getenv("SENTRY_DSN"),
	})
	if err != nil {
		panic(err)
	}
	defer sentry.Recover()
	defer sentry.Flush(2 * time.Second)

	err = storeActiveCases(ctx, "counties")
	if err != nil {
		panic(err)
	}

	return nil
}

func storeActiveCases(ctx context.Context, collectionPrefix string) error {
	db, err := createDBClient(ctx)
	if err != nil {
		sentry.CaptureException(err)
		return err
	}

	iter := db.Collection(fmt.Sprintf("%s-live", collectionPrefix)).Documents(ctx)
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
		go func(hub *sentry.Hub) {
			hub.ConfigureScope(func(scope *sentry.Scope) {
				scope.SetTag("fips", row.Fips)
				scope.SetTag("state", row.State)
				scope.SetTag("county", row.County)
			})

			if err := calculateActiveCases(ctx, db, collectionPrefix, row); err != nil {
				fmt.Println("failed to calculate and store active cases for", row.Fips, row.State, row.County)
				hub.CaptureException(err)
			}
			wg.Done()
		}(sentry.CurrentHub().Clone())
	}
	wg.Wait()

	return nil
}

func calculateActiveCases(ctx context.Context, db *firestore.Client, collectionPrefix string, row computedRow) error {
	historical := db.Collection(collectionPrefix + "-historical")
	live := db.Collection(collectionPrefix + "-live").Doc(row.Fips)
	api := db.Collection(collectionPrefix + "-api").Doc(row.Fips)

	currentCases, deaths, err := getCurrentNumbers(ctx, row.Fips, live)
	if err != nil {
		return err
	}

	// FIXME: This assumes there will always be deaths everywhere. Is that fair?
	calculatedDeaths := false
	if deaths == 0 {
		calculatedDeaths = true
		deaths = int(float32(currentCases) * 0.01)
	}

	results, err := getRelevantHistoricalCases(ctx, row.Fips, historical)
	if err != nil {
		return err
	}

	inputs := buildInputs(results)
	activeCaseCount := computeActiveCaseCount(
		currentCases,
		inputs[14].value,
		inputs[15].value,
		inputs[25].value,
		inputs[26].value,
		inputs[49].value,
		deaths,
	)

	newDeathsToday := deaths - inputs[1].deaths

	if calculatedDeaths {
		newDeathsToday = 0
	}

	// Create new entry into <resource>-api collection
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
		"CalculatedDeaths": calculatedDeaths,
		"ActiveCases":      activeCaseCount,
		"NewCasesToday":    currentCases - inputs[1].value,
		"NewDeathsToday":   newDeathsToday,
		"Score":            inputs[14].score + inputs[15].score + inputs[25].score + inputs[26].score + inputs[49].score,
	}, firestore.MergeAll); err != nil {
		return err
	}

	return nil
}

type calculation struct {
	value  int
	deaths int
	score  int
}

func buildInputs(rows []historicalRow) map[int]calculation {
	today := time.Now()
	targetDays := []int{1, 14, 15, 25, 26, 49}
	calculations := make(map[int]calculation)

	for _, row := range rows {
		t, _ := time.Parse(layoutISO, row.Date)

		// Days between today and current row
		dayDiff := today.Sub(t).Hours() / 24

		for _, targetDay := range targetDays {
			// the close the day diff is to the targetDay, the better(lower) the score
			score := int(dayDiff) - targetDay
			if score < 0 {
				score = -score
			}

			cases, err := strconv.Atoi(row.Cases)
			if err != nil {
				continue
			}

			deaths, err := strconv.Atoi(row.Deaths)
			if err != nil {
				continue
			}

			_, ok := calculations[targetDay]
			// each iteration, if we've found a day that's closer to our desired day difference, we update it
			if !ok || score < calculations[targetDay].score {
				calculations[targetDay] = calculation{
					value:  cases,
					deaths: deaths,
					score:  score,
				}
			}
		}
	}

	return calculations
}

// active case algorithm, taken from:
// https://www.esri.com/arcgis-blog/products/js-api-arcgis/mapping/animate-and-explore-covid-19-data-through-time/#active
func computeActiveCaseCount(current, days14, days15, days25, days26, days49, deaths int) int {
	return int(
		float32(current-days14) + (0.19 * float32(days15-days25)) + (0.05 * float32(days26-days49)) - float32(deaths),
	)
}

func getRelevantHistoricalCases(ctx context.Context, fips string, cases *firestore.CollectionRef) ([]historicalRow, error) {
	var relevantCases []historicalRow
	t := time.Now().Format(layoutISO)

	it := cases.
		Where("Date", "<=", t).
		Where("Fips", "==", fips).
		Limit(50).
		OrderBy("Date", firestore.Desc).
		Documents(ctx)

	for {
		doc, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return relevantCases, err
		}

		var report historicalRow
		err = doc.DataTo(&report)
		if err != nil {
			return relevantCases, err
		}

		relevantCases = append(relevantCases, report)
	}

	return relevantCases, nil
}

func getCurrentNumbers(ctx context.Context, fips string, doc *firestore.DocumentRef) (int, int, error) {
	docsnap, err := doc.Get(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to lookup document with current numbers - %e", err)
	}

	var d liveRow
	docsnap.DataTo(&d)

	cases, err := strconv.Atoi(d.Cases)
	if err != nil {
		return 0, 0, err
	}

	deaths, err := strconv.Atoi(d.Deaths)
	if err != nil {
		return cases, 0, nil
	}

	return cases, deaths, nil
}
