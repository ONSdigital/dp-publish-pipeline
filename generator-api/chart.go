package main

import (
	"encoding/csv"
	"encoding/json"
	"net/http"
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

func downloadChart(data []byte, format string, w http.ResponseWriter) {
	var chart Chart
	json.Unmarshal(data, &chart)
	csvFile := csv.NewWriter(w)
	csvFile.Write([]string{"Title", chart.Title})
	csvFile.Write([]string{"Notes", chart.Notes})
	headers := append(chart.Headers, "")
	csvFile.Write(headers)
	for _, set := range chart.Data {
		row := make([]string, 0)
		for _, header := range headers {
			row = append(row, set[header])
		}
		csvFile.Write(row[:len(row)-1])
	}
	csvFile.Flush()
}
