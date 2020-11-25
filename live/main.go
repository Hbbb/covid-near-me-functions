package p

import (
	"net/http"
)

type FipsRow interface {
	Fips() string
}

type CountyRow struct {
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

func ImportLive(collectionName string, url string) {
	resp, err := http.Get(url)


}

func ImportLiveCounties() {
	ImportLive("counties-current", "https://raw.githubusercontent.com/nytimes/covid-19-data/master/live/us-counties.csv")
}
