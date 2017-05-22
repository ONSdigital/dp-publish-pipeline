package utils

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/ONSdigital/go-ns/log"
)

type Page struct {
	Uri  string `json:"uri"`
	Type string `json:"type"`
}

func GetEnvironmentVariable(name string, defaultValue string) string {
	environmentValue := os.Getenv(name)
	if environmentValue != "" {
		return environmentValue
	}
	return defaultValue
}

func GetEnvironmentVariableAsArray(name string, defaultValue string) []string {
	environmentValue := os.Getenv(name)
	if environmentValue != "" {
		return strings.Split(environmentValue, ",")
	}
	return []string{defaultValue}
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
		log.ErrorC("unmarshalling json to Page", err, nil)
	}
	return page.Uri
}

func GetType(data []byte) string {
	var page Page
	err := json.Unmarshal(data, &page)
	if err != nil {
		log.ErrorC("unmarshalling json to Page", err, nil)
	}
	return page.Type
}
