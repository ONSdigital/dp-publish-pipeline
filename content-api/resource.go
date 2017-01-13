package main

import (
	"log"
	"net/http"

	"github.com/ONSdigital/dp-publish-pipeline/s3"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
)

func getResource(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	bucketName := utils.GetEnvironmentVariable("S3_BUCKET", "content")
	s3Client := s3.CreateClient(bucketName)
	data, err := s3Client.GetObject(uri)
	if err != nil {
		log.Printf("Resource not found uri %s", uri)
		http.Error(w, "Content not found", http.StatusNotFound)
	}
	w.Write(data)
}
