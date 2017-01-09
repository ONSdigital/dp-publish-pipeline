package main

import (
	"encoding/csv"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/ONSdigital/dp-publish-pipeline/xls"
)

type DataFilter struct {
	FromMonth   string
	ToMonth     string
	FromQuarter string
	ToQuarter   string
	FromYear    string
	ToYear      string
	Frequency   string
}

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
	Type        string                `json:"type"`
}

type Record struct {
	FileLocation string `json:"fileLocation" bson:"fileLocation"`
	FileContent  string `json:"fileContent" bson:"fileContent"`
	CollectionId string `json:"collectionId" bson:"collectionId"`
}

func generateTimeseries(data []byte, filter DataFilter, format string, w http.ResponseWriter) {
	var timeSeries TimeSeries
	json.Unmarshal(data, &timeSeries)
	if format == xlsFormat {
		utils.SetXLSContentHeader(w)
		xlsFile := xls.CreateXLSWorkbook("data")
		defer xlsFile.Close()
		timeSeriesToWriter(xlsFile.WriteRow, timeSeries, filter)
		xlsFile.DumpToWriter(w)
	} else if format == csvFormat {
		utils.SetCSVContentHeader(w)
		csv := csv.NewWriter(w)
		timeSeriesToWriter(csv.Write, timeSeries, filter)
		csv.Flush()
	}
}

func timeSeriesToWriter(writer fileWriter, timeSeries TimeSeries, filter DataFilter) {
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
				writer([]string{data.Date, data.Value})
			}
		}
	case "quarter":
		if filter.FromQuarter != "" && filter.ToQuarter != "" {
			filterOnQuarter(writer, timeSeries, filter)
		} else {
			for _, data := range timeSeries.Quarters {
				writer([]string{data.Date, data.Value})
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

func filterOnYears(fileWriter fileWriter, timeSeries TimeSeries, filter DataFilter) {
	min, _ := strconv.Atoi(filter.FromYear)
	max, _ := strconv.Atoi(filter.ToYear)
	for _, data := range timeSeries.Years {
		currentYear, _ := strconv.Atoi(data.Year)
		if currentYear >= min && currentYear <= max {
			fileWriter([]string{data.Date, data.Value})
		}
	}
}

func filterOnQuarter(fileWriter fileWriter, timeSeries TimeSeries, filter DataFilter) {
	min, _ := strconv.Atoi(filter.FromYear)
	max, _ := strconv.Atoi(filter.ToYear)
	for _, data := range timeSeries.Quarters {
		currentYear, _ := strconv.Atoi(data.Year)
		if currentYear > min && currentYear < max {
			fileWriter([]string{data.Date, data.Value})
		} else if currentYear == min {
			minQuarter := quarterToNumber(filter.FromQuarter)
			currentQuarter := quarterToNumber(data.Quarter)
			if currentQuarter >= minQuarter {
				fileWriter([]string{data.Date, data.Value})
			}
		} else if currentYear == max {
			maxQuarter := quarterToNumber(filter.ToQuarter)
			currentQuarter := quarterToNumber(data.Quarter)
			if currentQuarter <= maxQuarter {
				fileWriter([]string{data.Date, data.Value})
			}
		}

	}
}

func quarterToNumber(quarter string) int {
	value, _ := strconv.Atoi(string(quarter[1]))
	return value
}

func filterOnMonth(fileWriter fileWriter, timeSeries TimeSeries, filter DataFilter) {
	min, _ := strconv.Atoi(filter.FromYear)
	max, _ := strconv.Atoi(filter.ToYear)
	for _, data := range timeSeries.Months {
		currentYear, _ := strconv.Atoi(data.Year)
		if currentYear > min && currentYear < max {
			fileWriter([]string{data.Date, data.Value})
		} else if currentYear == min {
			minQuarter, _ := strconv.Atoi(filter.FromMonth)
			currentQuarter := monthToNumber(data.Month)
			if currentQuarter >= minQuarter {
				fileWriter([]string{data.Date, data.Value})
			}
		} else if currentYear == max {
			maxQuarter, _ := strconv.Atoi(filter.ToMonth)
			currentQuarter := monthToNumber(data.Month)
			if currentQuarter <= maxQuarter {
				fileWriter([]string{data.Date, data.Value})
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
