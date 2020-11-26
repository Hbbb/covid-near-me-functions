module github.com/hbbb/covid-near-me-functions

go 1.15

require github.com/hbbb/covid-near-me-functions/functions v0.0.0
replace github.com/hbbb/covid-near-me-functions/functions => ./functions

require (
	cloud.google.com/go/firestore v1.3.0
	google.golang.org/api v0.29.0
)
