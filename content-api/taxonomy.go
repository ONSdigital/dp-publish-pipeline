package main

import (
	"io/ioutil"
	"log"
	"net/http"
)

func getTaxonomy(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadFile("static-taxonomy.json")
	if err != nil {
		log.Printf("Error loading static file : %s", err.Error())
	}
	w.Write([]byte(data))
}