package main

import (
	"errors"
	"log"
	"net/http"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type fileWriter func([]string) error

const dataBase = "onswebsite"
const metaCollection = "meta"

const mongodbHost = "MONGODB"

const xlsFormat = "xls"
const csvFormat = "csv"

func downloadFile(w http.ResponseWriter, r *http.Request) {
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
	dbSession, err := mgo.Dial(utils.GetEnvironmentVariable(mongodbHost, "localhost"))
	if err != nil {
		return nil, err
	}
	defer dbSession.Close()
	db := dbSession.DB(dataBase)
	var record Record
	notFoundErr := db.C(metaCollection).Find(bson.M{"fileLocation": uri}).One(&record)
	if notFoundErr != nil {
		return nil, notFoundErr
	}
	return []byte(record.FileContent), nil
}

func main() {
	port := utils.GetEnvironmentVariable("PORT", "8081")
	http.HandleFunc("/generator", downloadFile)
	http.ListenAndServe(":"+port, nil)
}
