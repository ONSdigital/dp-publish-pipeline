package main

import (
	"io/ioutil"
	"log"
	"net/http"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
)

func main() {
	port := utils.GetEnvironmentVariable("PORT", "8092")
	data, err := ioutil.ReadFile("static-taxonomy.json")
	if err != nil {
		log.Printf("Error loading static file : %s", err.Error())
	}
	http.HandleFunc("/taxonomy", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(data))
	})
	http.ListenAndServe(":"+port, nil)
}
