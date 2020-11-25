package p

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
)

var day = time.Hour * 24

const (
	layoutISO = "2006-01-02"
	layoutUS  = "January 2, 2006"
)

func StoreActiveCasesForState(fips, date string) {
	ctx := context.Background()
	db, err := createDBClient(ctx)
	if err != nil {
		panic(err)
	}

	historical := db.Collection("states-historical")
	live := db.Collection("states-live").Doc(fips)

	currentCases, deaths := getLiveNumbersForState(ctx, fips, live)

	daysAgo14Cases := getCasesFromDaysAgo(ctx, fips, 14, "Cases", historical)
	daysAgo1Cases := getCasesFromDaysAgo(ctx, fips, 1, "Cases", historical)
	daysAgo1Deaths := getCasesFromDaysAgo(ctx, fips, 1, "Cases", historical)
	daysAgo15Cases := getCasesFromDaysAgo(ctx, fips, 15, "Cases", historical)
	daysAgo25Cases := getCasesFromDaysAgo(ctx, fips, 25, "Cases", historical)
	daysAgo26Cases := getCasesFromDaysAgo(ctx, fips, 26, "Cases", historical)
	daysAgo49Cases := getCasesFromDaysAgo(ctx, fips, 49, "Cases", historical)

	activeCaseCount := computeActiveCaseCount(currentCases,
		daysAgo14Cases,
		daysAgo15Cases,
		daysAgo25Cases,
		daysAgo26Cases,
		daysAgo49Cases,
		deaths)

	live.Set(ctx, map[string]interface{}{
		"ActiveCases":    activeCaseCount,
		"NewCasesToday":  currentCases - daysAgo1Cases,
		"NewDeathsToday": deaths - daysAgo1Deaths,
	}, firestore.MergeAll)
}

func ComputeNewCasesToday()
func ComputeNewDeathsToday()

func computeActiveCaseCount(current, days14, days15, days25, days26, days49, deaths int) float32 {
	return float32(current-days14) + (0.19 * float32(days15-days25)) + (0.05 * float32(days26-days49)) - float32(deaths)
}

func getCasesFromDaysAgo(ctx context.Context, fips string, daysAgo int, resource string, cases *firestore.CollectionRef) int {
	today := time.Now()
	date := today.AddDate(0, 0, -daysAgo).Format(layoutISO)

	docsnap, err := cases.Doc(fips + "_" + date).Get(ctx)
	if err != nil {
		panic(err)
	}

	raw, err := docsnap.DataAt(resource)
	if err != nil {
		panic(err)
	}

	return raw.(int)
}

func getLiveNumbersForState(ctx context.Context, fips string, doc *firestore.DocumentRef) (int, int) {
	docsnap, err := doc.Get(ctx)
	if err != nil {
		panic(err)
	}

	rawCases, err := docsnap.DataAt("Cases")
	if err != nil {
		panic(err)
	}

	rawDeaths, err := docsnap.DataAt("Deaths")
	if err != nil {
		panic(err)
	}

	return rawCases.(int), rawDeaths.(int)
}

func createDBClient(ctx context.Context) (*firestore.Client, error) {
	projectID := "covid-near-me-296621"

	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	return client, nil
}
