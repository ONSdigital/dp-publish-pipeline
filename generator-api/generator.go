package main

import (
	"database/sql"
	"errors"
	"log"
	"net/http"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	_ "github.com/lib/pq"
)

type fileWriter func([]string) error

const xlsFormat = "xls"
const csvFormat = "csv"

var db *sql.DB
var findMetaDataStatement *sql.Stmt

func generateFile(w http.ResponseWriter, r *http.Request) {
	format, uri, err := findParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	filter := findFilterParams(r)
	log.Printf("Got format : %s, uri: %s", format, uri)
	data, err := loadPageData(uri)
	if err != nil {
		http.Error(w, "Content not found", http.StatusNotFound)
		return
	}
	pageType := utils.GetType(data)
	log.Printf("page Type : %s", pageType)
	if pageType == "timeseries" {
		generateTimeseries(data, filter, format, w)
	} else if pageType == "chart" {
		generateChart(data, format, w)
	} else {
		http.Error(w, "Unsupported type", http.StatusBadRequest)
	}
}

func exportFiles(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	log.Printf("Exporting timeseries %+v as %s", r.PostForm["uri"], r.PostFormValue("format"))
	format := r.PostFormValue("format")
	uriList := r.PostForm["uri"]
	log.Printf("Export request! format :  %s", format)
	filter := findFilterParams(r)
	for _, item := range uriList {
		copydata(item, format, filter, w)
	}

	if format == csvFormat {
		utils.SetCSVContentHeader(w)
	} else {
		utils.SetXLSContentHeader(w)
	}
}

func copydata(uri string, format string, filter DataFilter, w http.ResponseWriter) {
	data, err := loadPageData(uri)
	if err != nil {
		http.Error(w, "Content not found", http.StatusNotFound)
		return
	}
	pageType := utils.GetType(data)
	log.Printf("page Type : %s", pageType)
	if pageType == "timeseries" {
		generateTimeseries(data, filter, format, w)
	} else if pageType == "chart" {
		generateChart(data, format, w)
	} else {
		http.Error(w, "Unsupported type", http.StatusBadRequest)
	}
}

func findParams(query *http.Request) (string, string, error) {
	format := query.URL.Query().Get("format")
	uri := query.URL.Query().Get("uri")
	if format != csvFormat && format != xlsFormat {
		return "", "", errors.New("Unsupported format : " + format)
	}
	if uri == "" {
		return "", "", errors.New("No uri provided")
	}
	return format, uri, nil
}

func findFilterParams(query *http.Request) DataFilter {
	var filter DataFilter
	filter.FromMonth = query.URL.Query().Get("fromMonth")
	filter.FromYear = query.URL.Query().Get("fromYear")
	filter.ToMonth = query.URL.Query().Get("toMonth")
	filter.ToYear = query.URL.Query().Get("toYear")
	filter.Frequency = query.URL.Query().Get("frequency")
	filter.FromQuarter = query.URL.Query().Get("fromQuarter")
	filter.ToQuarter = query.URL.Query().Get("toQuarter")
	log.Printf("Filter %+v", filter)
	return filter
}

func loadPageData(uri string) ([]byte, error) {
	results := findMetaDataStatement.QueryRow(uri + "?lang=en")
	var content sql.NullString
	err := results.Scan(&content)
	if err != nil {
		return nil, err
	}
	return []byte(content.String), nil
}

func dialDb(dbSource string) error {
	postgresDb, err := sql.Open("postgres", dbSource)
	db = postgresDb
	findMetaDataSQL := "SELECT content FROM metadata WHERE uri = $1"
	findMetaDataStatement = prepareSQLStatement(findMetaDataSQL, db)
	return err
}

func prepareSQLStatement(sql string, db *sql.DB) *sql.Stmt {
	statement, err := db.Prepare(sql)
	if err != nil {
		log.Panicf("Error: Could not prepare statement on database: %s", err.Error())
	}
	return statement
}

func main() {
	dbSource := utils.GetEnvironmentVariable("DB_ACCESS", "user=dp dbname=dp sslmode=disable")
	err := dialDb(dbSource)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer findMetaDataStatement.Close()

	port := utils.GetEnvironmentVariable("PORT", "8092")
	http.HandleFunc("/generator", generateFile)
	http.HandleFunc("/export", exportFiles)
	http.ListenAndServe(":"+port, nil)
}
