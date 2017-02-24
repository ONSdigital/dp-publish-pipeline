package utils

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
)

type Page struct {
	Uri  string `json:"uri"`
	Type string `json:"type"`
}

func GetEnvironmentVariable(name string, defaultValue string) string {
	environmentValue := os.Getenv(name)
	if environmentValue != "" {
		return environmentValue
	} else {
		return defaultValue
	}
}

func GetEnvironmentVariableInt(name string, defaultValue int) (int, error) {
	environmentValue := os.Getenv(name)
	if environmentValue != "" {
		return strconv.Atoi(environmentValue)
	} else {
		return defaultValue, nil
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
	err := json.Unmarshal(data, &page)
	if err != nil {
		log.Printf("Error unmarshalling json to Page : %s", err.Error())
	}
	return page.Uri
}

func GetType(data []byte) string {
	var page Page
	err := json.Unmarshal(data, &page)
	if err != nil {
		log.Printf("Error unmarshalling json to Page : %s", err.Error())
	}
	return page.Type
}
