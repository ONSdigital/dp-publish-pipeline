package main

import (
	"log"
	"net/http"

	mongo "github.com/ONSdigital/dp-publish-pipeline/mongodb"
)

func getData(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	lang := r.URL.Query().Get("lang")
	client, connectionErr := mongo.CreateClient()
	defer client.Close()
	if connectionErr != nil {
		log.Fatalf("Failed to connect to mongodb. Error : %s", connectionErr.Error())
	}
	document, notFound := client.FindPage(uri, lang)
	if notFound != nil {
		log.Printf("Data not found uri %s", uri)
		http.Error(w, "Content not found", http.StatusNotFound)
	}
	w.Write([]byte(document.FileContent))
}
