package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
)

type DataSet struct {
	FileLocation string
	FileContent  string
}

func storeData(jsonMessage []byte) {
	var dataSet DataSet
	err := json.Unmarshal(jsonMessage, &dataSet)
	if err != nil {
		log.Printf("Failed to parse json message")
	} else {
		rootDirectory := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", ".")
		path := filepath.Join(rootDirectory, dataSet.FileLocation)
		dirErr := os.MkdirAll(filepath.Dir(path), 0774) // Read + Exec
		if dirErr == nil {
			fileHandle, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0744)
			defer fileHandle.Close()
			if err == nil {
				_, writeErr := fileHandle.WriteString(dataSet.FileContent)
				if writeErr == nil {
					log.Printf("Written file at %s", path)
				} else {
					log.Printf("Failed to write to a file. Error : %v", writeErr)
				}
			} else {
				log.Printf("Failed to create file handle. Error : %v", err)
			}
		} else {
			log.Printf("Failed to create directory. Error : %v", dirErr)
		}
	}
}

func main() {
	log.Printf("Starting publish receiver")
	master := kafka.CreateConsumer()
	defer func() {
		err := master.Close()
		if err != nil {
			panic(err)
		}
	}()
	topic := utils.GetEnvironmentVariable("TOPIC", "test")
	kafka.ProcessMessages(master, topic, storeData)
	log.Printf("Service stopped")
}
