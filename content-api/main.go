package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
)

func main() {
	port := utils.GetEnvironmentVariable("PORT", "8082")
	generatorURL := utils.GetEnvironmentVariable("GENERATOR_URL", "localhost:8092")
	log.Printf("Starting Content API on port : %s", port)
	http.HandleFunc("/data", getData)
	http.HandleFunc("/parent", getParent)
	http.HandleFunc("/resource", getResource)
	http.HandleFunc("/taxonomy", getTaxonomy)

	http.HandleFunc("/generator", func(w http.ResponseWriter, r *http.Request) {
		res, _ := http.Get("http://" + generatorURL + r.URL.String())
		body, _ := ioutil.ReadAll(res.Body)
		if strings.Contains(r.URL.Query().Get("format"), "csv") {
			utils.SetCSVContentHeader(w)
		} else {
			utils.SetXLSContentHeader(w)
		}
		w.Write(body)
	})
	http.ListenAndServe(":"+port, nil)
}
