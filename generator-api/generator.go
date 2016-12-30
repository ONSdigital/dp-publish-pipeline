package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
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

type DataFilter struct {
	FromMonth   string
	ToMonth     string
	FromQuarter string
	ToQuarter   string
	FromYear    string
	ToYear      string
	Frequency   string
}

type Record struct {
	FileLocation string `json:"fileLocation" bson:"fileLocation"`
	FileContent  string `json:"fileContent" bson:"fileContent"`
	CollectionId string `json:"collectionId" bson:"collectionId"`
}

const DATA_BASE = "onswebsite"
const MASTER_COLLECTION = "meta"

const MONGODB_ENV = "MONGODB"

func downloadFile(w http.ResponseWriter, r *http.Request) {
	format, uri, _ := findParams(r)
	filter := findFilterParams(r)
	log.Printf("Got format : %s, uri: %s", format, uri)
	timeSeries := loadTimeSeriesData(uri)
	//w.Header().Set("Content-Disposition", "attachment; filename=data.csv")
	//w.Header().Set("Content-Type", "text/csv")
	timeSeriesToCSVFile(w, timeSeries, filter)
}

func findParams(query *http.Request) (string, string, error) {
	format := query.URL.Query().Get("format")
	uri := query.URL.Query().Get("uri")
	if format != "csv" {
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

func timeSeriesToCSVFile(writer io.Writer, timeSeries TimeSeries, filter DataFilter) {
	csv := csv.NewWriter(writer)
	csv.Write([]string{"Title", timeSeries.Description.Title})
	csv.Write([]string{"CDID", timeSeries.Description.Cdid})
	csv.Write([]string{"PreUnit", timeSeries.Description.PreUnit})
	csv.Write([]string{"Unit", timeSeries.Description.Unit})
	csv.Write([]string{"Release date", timeSeries.Description.ReleaseDate})
	csv.Write([]string{"Next release", timeSeries.Description.NextRelease})
	csv.Write([]string{"Important Notes", ""})

	switch filter.Frequency {
	case "year":
		if filter.FromYear != "" && filter.ToYear != "" {
			filterOnYears(csv, timeSeries, filter)
		} else {
			for _, data := range timeSeries.Years {
				csv.Write([]string{data.Year, data.Value})
			}
		}
	case "quarter":
		if filter.FromQuarter != "" && filter.ToQuarter != "" {
			filterOnQuarter(csv, timeSeries, filter)
		} else {
			for _, data := range timeSeries.Quarters {
				csv.Write([]string{data.Year + " " + data.Quarter, data.Value})
			}
		}
	case "month":
		if filter.FromMonth != "" && filter.ToMonth != "" {
			filterOnMonth(csv, timeSeries, filter)
		} else {
			for _, data := range timeSeries.Months {
				csv.Write([]string{data.Date, data.Value})
			}
		}
	}

	// Wrtie all the data to the writer
	csv.Flush()
}

func filterOnYears(csv *csv.Writer, timeSeries TimeSeries, filter DataFilter) {
	min, _ := strconv.Atoi(filter.FromYear)
	max, _ := strconv.Atoi(filter.ToYear)
	for _, data := range timeSeries.Years {
		currentYear, _ := strconv.Atoi(data.Year)
		if currentYear >= min && currentYear <= max {
			csv.Write([]string{data.Year, data.Value})
		}
	}
}

func filterOnQuarter(csv *csv.Writer, timeSeries TimeSeries, filter DataFilter) {
	min, _ := strconv.Atoi(filter.FromYear)
	max, _ := strconv.Atoi(filter.ToYear)
	for _, data := range timeSeries.Quarters {
		currentYear, _ := strconv.Atoi(data.Year)
		if currentYear > min && currentYear < max {
			csv.Write([]string{data.Year + " " + data.Quarter, data.Value})
		} else if currentYear == min {
			minQuarter := quarterToNumber(filter.FromQuarter)
			currentQuarter := quarterToNumber(data.Quarter)
			if currentQuarter >= minQuarter {
				csv.Write([]string{data.Year + " " + data.Quarter, data.Value})
			}
		} else if currentYear == max {
			maxQuarter := quarterToNumber(filter.ToQuarter)
			currentQuarter := quarterToNumber(data.Quarter)
			if currentQuarter <= maxQuarter {
				csv.Write([]string{data.Year + " " + data.Quarter, data.Value})
			}
		}

	}
}

func quarterToNumber(quarter string) int {
	value, _ := strconv.Atoi(string(quarter[1]))
	return value
}

func filterOnMonth(csv *csv.Writer, timeSeries TimeSeries, filter DataFilter) {
	min, _ := strconv.Atoi(filter.FromYear)
	max, _ := strconv.Atoi(filter.ToYear)
	for _, data := range timeSeries.Months {
		currentYear, _ := strconv.Atoi(data.Year)
		if currentYear > min && currentYear < max {
			csv.Write([]string{data.Date, data.Value})
		} else if currentYear == min {
			minQuarter, _ := strconv.Atoi(filter.FromMonth)
			currentQuarter := monthToNumber(data.Month)
			if currentQuarter >= minQuarter {
				csv.Write([]string{data.Date, data.Value})
			}
		} else if currentYear == max {
			maxQuarter, _ := strconv.Atoi(filter.ToMonth)
			currentQuarter := monthToNumber(data.Month)
			if currentQuarter <= maxQuarter {
				csv.Write([]string{data.Date, data.Value})
			}
		}

	}
}

func monthToNumber(month string) int {
	lowerCase := strings.ToLower(month)
	switch lowerCase {
	case "january":
		return 1
	case "february":
		return 2
	case "march":
		return 3
	case "april":
		return 4
	case "may":
		return 5
	case "june":
		return 6
	case "july":
		return 7
	case "august":
		return 8
	case "september":
		return 9
	case "october":
		return 10
	case "november":
		return 11
	case "december":
		return 12
	}

	return 0
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
