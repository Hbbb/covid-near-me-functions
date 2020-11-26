package main

import (
	"fmt"
	"time"
)

func main() {
	// ctx := context.Background()
	// p.ImportHistoricalCounty(ctx, "")
	// p.StoreActiveCasesForCounty(ctx, "")
	// p.ImportLiveCounties()

	// importPopulations()

	loc, err := time.LoadLocation("America/New_York")

	if err != nil {
		panic(err)
	}
	// t := time.Now().In(loc)
	// log.Println(t.Zone())
	fmt.Println("now", time.Now().Format("2006-01-02 15:04 z-07:00"))
	fmt.Println("zone", time.Now().In(loc).Format("2006-01-02 15:04 z-07:00"))
	fmt.Println("utc", time.Now().UTC().Format("2006-01-02 15:04 z-07:00"))

}
