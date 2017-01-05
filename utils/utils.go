package utils

import (
	"encoding/json"
	"net/http"
	"os"
)

type Page struct {
	Uri  string `json:"uri"`  // Uri of the page
	Type string `json:"type"` // Type of page timeseries, chart, ...
}

func GetEnvironmentVariable(name string, defaultValue string) string {
	environmentValue := os.Getenv(name)
	if environmentValue != "" {
		return environmentValue
	} else {
		return defaultValue
	}
}

func SetCSVContentHeader(w http.ResponseWriter) {
	w.Header().Set("Content-Disposition", "attachment; filename=data.csv")
	w.Header().Set("Content-Type", "text/csv")
}

func SetXLSContentHeader(w http.ResponseWriter) {
	w.Header().Set("Content-Disposition", "attachment; filename=data.xls")
	w.Header().Set("Content-Type", "application/vnd.ms-excel")
}

func GetUri(data []byte) string {
	var page Page
	json.Unmarshal(data, &page)
	return page.Uri
}

func GetType(data []byte) string {
	var page Page
	json.Unmarshal(data, &page)
	return page.Type
}
