package main

import (
	"database/sql"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	_ "github.com/lib/pq"
)

var findMetaDataStatement *sql.Stmt
var findS3DataStatement *sql.Stmt

func exportHandler(w http.ResponseWriter, r *http.Request) {
	generatorURL := utils.GetEnvironmentVariable("GENERATOR_URL", "localhost:8092")
	r.ParseForm()
	res, _ := http.PostForm("http://"+generatorURL+r.URL.String(), r.PostForm)
	body, _ := ioutil.ReadAll(res.Body)
	if strings.Contains(r.PostFormValue("format"), "csv") {
		utils.SetCSVContentHeader(w)
	} else {
		utils.SetXLSContentHeader(w)
	}
	w.Write(body)
}

func generatorHandler(w http.ResponseWriter, r *http.Request) {
	generatorURL := utils.GetEnvironmentVariable("GENERATOR_URL", "localhost:8092")
	res, _ := http.Get("http://" + generatorURL + r.URL.String())
	body, _ := ioutil.ReadAll(res.Body)
	if strings.Contains(r.URL.Query().Get("format"), "csv") {
		utils.SetCSVContentHeader(w)
	} else {
		utils.SetXLSContentHeader(w)
	}
	w.Write(body)
}

func prepareSQLStatement(sql string, db *sql.DB) *sql.Stmt {
	statement, err := db.Prepare(sql)
	if err != nil {
		log.Panicf("Error: Could not prepare statement on database: %s", err.Error())
	}
	return statement
}

func main() {
	dbSource := utils.GetEnvironmentVariable("DB_ACCESS", "user=dp dbname=dp sslmode=disable")
	port := utils.GetEnvironmentVariable("PORT", "8082")
	db, err := sql.Open("postgres", dbSource)
	if err != nil {
		log.Panicf("DB open error: %s", err.Error())
	}
	defer db.Close()
	findMetaDataSQL := "SELECT content FROM metadata WHERE uri = $1"
	findS3DataSQL := "SELECT s3 FROM s3data WHERE uri = $1"
	findMetaDataStatement = prepareSQLStatement(findMetaDataSQL, db)
	findS3DataStatement = prepareSQLStatement(findS3DataSQL, db)
	defer findMetaDataStatement.Close()
	defer findS3DataStatement.Close()

	log.Printf("Starting Content API on port : %s", port)
	http.HandleFunc("/data", getData)
	http.HandleFunc("/parent", getParent)
	http.HandleFunc("/resource", getResource)
	http.HandleFunc("/taxonomy", getTaxonomy)
	http.HandleFunc("/generator", generatorHandler)
	http.HandleFunc("/export", exportHandler)
	http.ListenAndServe(":"+port, nil)
}
