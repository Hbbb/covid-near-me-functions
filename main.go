package main

import (
	"context"

	p "github.com/hbbb/covid-near-me-functions/compute"
)

func main() {
	ctx := context.Background()
	// p.ImportHistoricalCounty(ctx, "")
	p.StoreActiveCasesForCounty(ctx, "")
}
