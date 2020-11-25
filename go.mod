module github.com/hbbb/covid-near-me-functions

go 1.15

require github.com/hbbb/covid-near-me-functions/states v0.0.0
replace github.com/hbbb/covid-near-me-functions/states => ./states

require github.com/hbbb/covid-near-me-functions/live v0.0.0
replace github.com/hbbb/covid-near-me-functions/live => ./live


require github.com/hbbb/covid-near-me-functions/compute v0.0.0
replace github.com/hbbb/covid-near-me-functions/compute => ./compute