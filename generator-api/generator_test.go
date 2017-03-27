package generator

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const timeseriesData = "test-data/timeseries.json"
const chartData = "test-data/chart.json"
const localAddress = "http://localhost/generator"

func TestTimeseriesCsvYear(t *testing.T) {
	t.Parallel()
	if isPostgresAvailable() {
		Convey("with a url and param, test timeseries to csv with year filter", t, func() {
			url := "/timeseries1"
			AddTestData(url, loadTimeseries(timeseriesData))
			param := "?uri=" + url + "&frequency=years&format=csv&fromYear=1988&toYear=2000"
			r, _ := http.NewRequest("GET", localAddress+param, nil)
			w := httptest.NewRecorder()
			generateFile(w, r)
			csv := string(w.Body.Bytes())
			So(w.Code, ShouldEqual, 200)
			So(csv, ShouldContainSubstring, "Title,OS visits to EU:All visits Thousands-NSA")
			So(csv, ShouldContainSubstring, "1988,")
			So(csv, ShouldContainSubstring, "2000,")
			So(csv, ShouldNotContainSubstring, "1987,")
			So(csv, ShouldNotContainSubstring, "2001,")
		})
	} else {
		t.Skip("No mongodb connection available")
	}
}

func TestTimeseriesCsvQuarter(t *testing.T) {
	t.Parallel()
	if isPostgresAvailable() {
		Convey("with a url and param, test timeseries to csv with quarter filter", t, func() {
			url := "/timeseries2"
			AddTestData(url, loadTimeseries(timeseriesData))
			param := "?uri=" + url + "&frequency=quarters&format=csv&fromYear=1988&toYear=2000&fromQuarter=q3&toQuarter=q2"
			r, _ := http.NewRequest("GET", localAddress+param, nil)
			w := httptest.NewRecorder()
			generateFile(w, r)
			csv := string(w.Body.Bytes())
			So(w.Code, ShouldEqual, 200)
			So(csv, ShouldContainSubstring, "Title,OS visits to EU:All visits Thousands-NSA")
			So(csv, ShouldContainSubstring, "1988 Q3,")
			So(csv, ShouldContainSubstring, "2000 Q2,")
			So(csv, ShouldNotContainSubstring, "1988 Q2,")
			So(csv, ShouldNotContainSubstring, "2000 Q3,")
		})
	} else {
		t.Skip("No mongodb connection available")
	}
}

func TesTimeseriesCsvMonth(t *testing.T) {
	t.Parallel()
	if isPostgresAvailable() {
		Convey("with a url and param, test timeseries to csv with month filter", t, func() {
			url := "/timeseries3"
			AddTestData(url, loadTimeseries(timeseriesData))
			param := "?uri=" + url + "&frequency=months&format=csv&fromYear=1988&toYear=2000&fromMonth=06&toMonth=11"
			r, _ := http.NewRequest("GET", localAddress+param, nil)
			w := httptest.NewRecorder()
			generateFile(w, r)
			csv := string(w.Body.Bytes())
			So(w.Code, ShouldEqual, 200)
			So(csv, ShouldContainSubstring, "Title,OS visits to EU:All visits Thousands-NSA")
			So(csv, ShouldContainSubstring, "1988 JUN,")
			So(csv, ShouldContainSubstring, "2000 NOV,")
			So(csv, ShouldNotContainSubstring, "1988 MAY,")
			So(csv, ShouldNotContainSubstring, "2000 DEC,")
		})
	} else {
		t.Skip("No mongodb connection available")
	}
}

func TestChartCsv(t *testing.T) {
	t.Parallel()
	if isPostgresAvailable() {
		Convey("with a url and param, test chart to csv", t, func() {
			url := "/chart1"
			AddTestData(url, loadChart(chartData))
			param := "?uri=" + url + "&format=csv"
			r, _ := http.NewRequest("GET", localAddress+param, nil)
			w := httptest.NewRecorder()
			generateFile(w, r)
			csv := string(w.Body.Bytes())
			So(w.Code, ShouldEqual, 200)
			So(csv, ShouldContainSubstring, "Figure 4: National identity, England and Wales, 2011")
			So(csv, ShouldContainSubstring, "England and Wales,67.1,4.3,29.1,9.8")
			So(csv, ShouldContainSubstring, "South West,75.7,1.6,27.3,8.3")
		})
	} else {
		t.Skip("No mongodb connection available")
	}
}

func TestChartXls(t *testing.T) {
	t.Parallel()
	if isPostgresAvailable() {
		Convey("with a url and param, test chart to xls", t, func() {
			url := "/chart2"
			AddTestData(url, loadChart(chartData))
			param := "?uri=" + url + "&format=xls"
			r, _ := http.NewRequest("GET", localAddress+param, nil)
			w := httptest.NewRecorder()
			generateFile(w, r)
			So(w.Code, ShouldEqual, 200)
			// Not the best way to test the xls file
			So(len(w.Body.Bytes()), ShouldEqual, 13824)
		})
	} else {
		t.Skip("No mongodb connection available")
	}
}

func TestTimeseriesXls(t *testing.T) {
	t.Parallel()
	if isPostgresAvailable() {
		Convey("with a url and param, test timeseries to xls", t, func() {
			url := "/timeseries4"
			AddTestData(url, loadTimeseries(timeseriesData))
			param := "?uri=" + url + "&format=xls&frequency=months"
			r, _ := http.NewRequest("GET", localAddress+param, nil)
			w := httptest.NewRecorder()
			generateFile(w, r)
			So(w.Code, ShouldEqual, 200)
			// Not the best way to test the xls file
			So(len(w.Body.Bytes()), ShouldEqual, 38400)
		})
	} else {
		t.Skip("No mongodb connection available")
	}
}

func TestTimeseriesExport(t *testing.T) {
	t.Parallel()
	if isPostgresAvailable() {
		Convey("with a url and param, test timeseries to csv with year filter", t, func() {
			url := "/timeseries8"
			AddTestData(url, loadTimeseries(timeseriesData))
			param := "?uri=" + url + "&format=csv"
			r, _ := http.NewRequest("POST", localAddress+param, nil)
			r.ParseForm()
			r.PostForm["uri"] = []string{url, url}
			r.PostForm["format"] = []string{"csv"}
			w := httptest.NewRecorder()
			exportFiles(w, r)
			csv := string(w.Body.Bytes())
			So(w.Code, ShouldEqual, 200)
			So(csv, ShouldContainSubstring, "Title,OS visits to EU:All visits Thousands-NSA")
			So(len(csv), ShouldEqual, 12174)
		})
	} else {
		t.Skip("No mongodb connection available")
	}
}

func TestUriNotFound(t *testing.T) {
	t.Parallel()
	if isPostgresAvailable() {
		Convey("with a invalid url, 404 code is returned", t, func() {
			url := "/notfound"
			param := "?uri=" + url + "&format=xls&frequency=month"
			r, _ := http.NewRequest("GET", localAddress+param, nil)
			w := httptest.NewRecorder()
			generateFile(w, r)
			So(w.Code, ShouldEqual, 404)
		})
	} else {
		t.Skip("No mongodb connection available")
	}
}

func TestNoParameters(t *testing.T) {
	t.Parallel()
	if isPostgresAvailable() {
		Convey("with a no parameters, 400 code is returned", t, func() {
			r, _ := http.NewRequest("GET", localAddress, nil)
			w := httptest.NewRecorder()
			generateFile(w, r)
			So(w.Code, ShouldEqual, 400)
		})
	} else {
		t.Skip("No mongodb connection available")
	}
}

func TestExtro(t *testing.T) {
	if isPostgresAvailable() {
		DropTestData()
	}
}

func isPostgresAvailable() bool {
	err := dialDb("user=dp dbname=dp sslmode=disable")
	if err != nil {
		return false
	}
	return true
}

func loadTimeseries(file string) []byte {
	d, _ := ioutil.ReadFile(file)
	var t TimeSeries
	json.Unmarshal(d, &t)
	data, _ := json.Marshal(t)
	return data
}

func loadChart(file string) []byte {
	d, _ := ioutil.ReadFile(file)
	var c Chart
	json.Unmarshal(d, &c)
	data, _ := json.Marshal(c)
	return data
}

func AddTestData(uri string, data []byte) {
	insertMetaDataSQL := "INSERT INTO metadata(collection_id, uri, content) VALUES($1, $2, $3)"
	insertMetaDataStatement := prepareSQLStatement(insertMetaDataSQL, db)
	defer insertMetaDataStatement.Close()
	result, err := insertMetaDataStatement.Query("test", uri+"?lang=en", string(data))
	if err != nil {
		log.Panicf("Failed to add test data. %s", err.Error())
	} else {
		result.Close()
	}
}

func DropTestData() {
	deleteMetaDataRowSQL := "DELETE from metadata where collection_id = $1"
	deleteMetaDataRowStatement := prepareSQLStatement(deleteMetaDataRowSQL, db)
	defer deleteMetaDataRowStatement.Close()
	result, err := deleteMetaDataRowStatement.Query("test")
	if err != nil {
		log.Panicf("Failed to remove test data. %s", err.Error())
	} else {
		result.Close()
	}
}
