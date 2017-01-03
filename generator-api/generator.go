package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type TimeSeriesValue struct {
	Date          string    `json:"date"`
	Value         string    `json:"value"`
	Year          string    `json:"year"`
	Month         string    `json:"month"`
	Quarter       string    `json:"quarter"`
	SourceDataset string    `json:"sourceDataset"`
	UpdateDate    time.Time `json:"updateDate"`
}

type TimeSeriesDescription struct {
	Title             string `json:"title"`
	NationalStatistic bool   `json:"nationalStatistic"`
	LatestRelease     bool   `json:"latestRelease"`
	Contact           struct {
		Email     string `json:"email"`
		Name      string `json:"name"`
		Telephone string `json:"telephone"`
	} `json:"contact"`
	ReleaseDate        string `json:"releaseDate"`
	NextRelease        string `json:"nextRelease"`
	Cdid               string `json:"cdid"`
	Unit               string `json:"unit"`
	PreUnit            string `json:"preUnit"`
	Source             string `json:"source"`
	SeasonalAdjustment string `json:"seasonalAdjustment"`
	Date               string `json:"date"`
	Number             string `json:"number"`
	SampleSize         int    `json:"sampleSize"`
}

type TimeSeries struct {
	Years       []TimeSeriesValue     `json:"years"`
	Quarters    []TimeSeriesValue     `json:"quarters"`
	Months      []TimeSeriesValue     `json:"months"`
	Description TimeSeriesDescription `json:"description"`
}

type Record struct {
	FileLocation string `json:"fileLocation" bson:"fileLocation"`
	FileContent  string `json:"fileContent" bson:"fileContent"`
	CollectionId string `json:"collectionId" bson:"collectionId"`
}

type FileWriter func([]string) error

const DATA_BASE = "onswebsite"
const MASTER_COLLECTION = "meta"

const MONGODB_ENV = "MONGODB"

func downloadFile(w http.ResponseWriter, r *http.Request) {
	format, uri, _ := findParams(r)
	filter := findFilterParams(r)
	log.Printf("Got format : %s, uri: %s", format, uri)
	timeSeries := loadTimeSeriesData(uri)

	if format == "xls" {
		w.Header().Set("Content-Disposition", "attachment; filename=data.xls")
		w.Header().Set("Content-Type", "application/vnd.ms-excel")
		xls := createXLSWorkbook("data")
		defer xls.close()
		timeSeriesToFile(xls.writeRow, timeSeries, filter)
		xls.dumpToWriter(w)
	} else {
		w.Header().Set("Content-Disposition", "attachment; filename=data.csv")
		w.Header().Set("Content-Type", "text/csv")
		csv := csv.NewWriter(w)
		timeSeriesToFile(csv.Write, timeSeries, filter)
		csv.Flush()
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

func loadTimeSeriesData(uri string) TimeSeries {
	var timeSeries TimeSeries
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
	json.Unmarshal([]byte(record.FileContent), &timeSeries)
	return timeSeries
}

func timeSeriesToFile(writer FileWriter, timeSeries TimeSeries, filter DataFilter) {
	writer([]string{"Title", timeSeries.Description.Title})
	writer([]string{"CDID", timeSeries.Description.Cdid})
	writer([]string{"PreUnit", timeSeries.Description.PreUnit})
	writer([]string{"Unit", timeSeries.Description.Unit})
	writer([]string{"Release date", timeSeries.Description.ReleaseDate})
	writer([]string{"Next release", timeSeries.Description.NextRelease})
	writer([]string{"Important Notes", ""})

	switch filter.Frequency {
	case "year":
		if filter.FromYear != "" && filter.ToYear != "" {
			filterOnYears(writer, timeSeries, filter)
		} else {
			for _, data := range timeSeries.Years {
				writer([]string{data.Year, data.Value})
			}
		}
	case "quarter":
		if filter.FromQuarter != "" && filter.ToQuarter != "" {
			filterOnQuarter(writer, timeSeries, filter)
		} else {
			for _, data := range timeSeries.Quarters {
				writer([]string{data.Year + " " + data.Quarter, data.Value})
			}
		}
	case "month":
		if filter.FromMonth != "" && filter.ToMonth != "" {
			filterOnMonth(writer, timeSeries, filter)
		} else {
			for _, data := range timeSeries.Months {
				writer([]string{data.Date, data.Value})
			}
		}
	}
}

func main() {
	http.HandleFunc("/generator", downloadFile)
	http.ListenAndServe(":8081", nil)
	//UploadTimeSeriesToMongo("data.json")
}

// Function used to upload test data into mongodb
func UploadTimeSeriesToMongo(file string) {
	d, _ := ioutil.ReadFile(file)
	var t TimeSeries
	json.Unmarshal(d, &t)
	data, _ := json.Marshal(t)
	var record Record
	record.CollectionId = "DataSet-456456"
	record.FileLocation = "/peoplepopulationandcommunity/leisureandtourism/timeseries/all/data.json"
	record.FileContent = string(data)
	dbSession, err := mgo.Dial(utils.GetEnvironmentVariable(MONGODB_ENV, "localhost"))
	if err != nil {
		panic(err)
	}
	defer dbSession.Close()
	db := dbSession.DB(DATA_BASE)
	db.C("meta").Insert(record)
}
