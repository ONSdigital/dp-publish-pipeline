package utils

import (
	"encoding/json"
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
