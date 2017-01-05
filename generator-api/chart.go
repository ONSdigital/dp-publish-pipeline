package main

import (
	"encoding/csv"
	"encoding/json"
	"net/http"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/ONSdigital/dp-publish-pipeline/xls"
)

type Chart struct {
	Type        string              `json:"type"`
	Title       string              `json:"title"`
	Filename    string              `json:"filename"`
	URI         string              `json:"uri"`
	Subtitle    string              `json:"subtitle"`
	Unit        string              `json:"unit"`
	Source      string              `json:"source"`
	Legend      string              `json:"legend"`
	HideLegend  bool                `json:"hideLegend"`
	Notes       string              `json:"notes"`
	AltText     string              `json:"altText"`
	Data        []map[string]string `json:"data"`
	Headers     []string            `json:"headers"`
	Series      []string            `json:"series"`
	Categories  []string            `json:"categories"`
	AspectRatio string              `json:"aspectRatio"`
	ChartType   string              `json:"chartType"`
	Files       []struct {
		Type     string `json:"type"`
		Filename string `json:"filename"`
	} `json:"files"`
}

func generateChart(data []byte, format string, w http.ResponseWriter) {
	var chart Chart
	json.Unmarshal(data, &chart)
	if format == csvFormat {
		utils.SetCSVContentHeader(w)
		csvFile := csv.NewWriter(w)
		chartToWriter(csvFile.Write, chart)
		csvFile.Flush()
	} else if format == xlsFormat {
		utils.SetXLSContentHeader(w)
		wb := xls.CreateXLSWorkbook("data")
		defer wb.Close()
		chartToWriter(wb.WriteRow, chart)
		wb.DumpToWriter(w)
	}

}

func chartToWriter(writer FileWriter, chart Chart) {
	writer([]string{"Title", chart.Title})
	writer([]string{"Notes", chart.Notes})
	headers := chart.Headers
	writer(headers)
	for _, set := range chart.Data {
		row := make([]string, 0)
		for _, header := range headers {
			row = append(row, set[header])
		}
		writer(row)
	}
}
