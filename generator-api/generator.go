package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type FileWriter func([]string) error

const DATA_BASE = "onswebsite"
const MASTER_COLLECTION = "meta"

const MONGODB_ENV = "MONGODB"

func downloadFile(w http.ResponseWriter, r *http.Request) {
	format, uri, _ := findParams(r)
	filter := findFilterParams(r)
	log.Printf("Got format : %s, uri: %s", format, uri)
	data := loadPageData(uri)
	pageType := utils.GetType(data)
	log.Printf("page Type : %s", pageType)
	if pageType == "timeseries" {
		downloadTimeseries(data, filter, format, w)
	} else if pageType == "chart" {
		downloadChart(data, format, w)
	}
}

func findParams(query *http.Request) (string, string, error) {
	format := query.URL.Query().Get("format")
	uri := query.URL.Query().Get("uri")
	if format != "csv" && format != "xls" {
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

func loadPageData(uri string) []byte {
	dbSession, err := mgo.Dial(utils.GetEnvironmentVariable(MONGODB_ENV, "localhost"))
	if err != nil {
		panic(err)
	}
	defer dbSession.Close()
	db := dbSession.DB(DATA_BASE)
	var record Record
	foundErr := db.C(MASTER_COLLECTION).Find(bson.M{"fileLocation": uri}).One(&record)
	if foundErr != nil {
		log.Panicf("Mongodb error : %s", foundErr.Error())
	}
	return []byte(record.FileContent)
}

func main() {
	http.HandleFunc("/generator", downloadFile)
	http.ListenAndServe(":8081", nil)
	//UploadTimeSeriesToMongo("chart.json")
}

// Function used to upload test data into mongodb
func UploadTimeSeriesToMongo(file string) {
	d, _ := ioutil.ReadFile(file)
	var t Chart
	json.Unmarshal(d, &t)
	data, _ := json.Marshal(t)
	var record Record
	record.CollectionId = "DataSet-456456"
	record.FileLocation = "/chart"
	record.FileContent = string(data)
	dbSession, err := mgo.Dial(utils.GetEnvironmentVariable(MONGODB_ENV, "localhost"))
	if err != nil {
		panic(err)
	}
	defer dbSession.Close()
	db := dbSession.DB(DATA_BASE)
	db.C("meta").Insert(record)
}
