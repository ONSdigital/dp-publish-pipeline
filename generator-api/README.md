### Generator API
Generate csv and xls from timeseries and chart data types.

### Setup
Need to run these, only once:

* `brew install xlslib`
* `go get github.com/satori/go.uuid`

### Environment variables
* `MONGODB_HOST` defaults to "localhost"
* `PORT` defaults to "8081"

### Interface

End point : `/generator`

#### Query Parameters
* `uri`: Location of the data
* `format`: Format to generator this can be csv / xls
* `fromYear`/`toYear`: Filter the data on the year
* `fromQuarter`/`toQuarter`: Filter the data on the quarter
* `fromMonth`/`toMonth`: Filter the data on the months
* `frequency`: How to present the data, this can be year, month and quarter
