### Generator API
Generate csv and xls from timeseries and chart data types.

### Setup
Need to run these, only once:

* `brew install xlslib`
* `brew install postgres`
* `go get github.com/satori/go.uuid`

### Environment variables
* `DB_ACCESS` defaults to "user=dp dbname=dp sslmode=disable"
* `PORT` defaults to "8092"

### Interface

End points : `/generator`, `/export`

#### Query Parameters for `/generator`
* `uri`: Location of the data
* `format`: Format to generator this can be csv / xls
* `fromYear`/`toYear`: Filter the data on the year
* `fromQuarter`/`toQuarter`: Filter the data on the quarter
* `fromMonth`/`toMonth`: Filter the data on the months
* `frequency`: How to present the data, this can be year, month and quarter

#### Query Parameters for `/export`
* `uri`: List of uris to export. eg "/timeseries1,/timeseries2"
