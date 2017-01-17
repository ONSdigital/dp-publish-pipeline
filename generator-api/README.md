### Generator API
Generate csv and xls from timeseries and chart data types.

### Setup
Need to run these, only once:
``brew install xlslib```

### Environment variables
* `MONGODB_HOST` defaults to "localhost"
* `PORT` defaults to "8081"

### Interface

End point : /generator

#### Parameters
* uri : Location of the data.
* format : Format to generator this can be csv / xls.
* from/to year: Filter the data on the year.
* from/to quarter: Filter the data on the quarter.
* from/to month: Filter the data on the months.
* frequency : How to present the data, this can be year, month and quarter.
