package main

import (
	"encoding/json"
	"log"
	"path/filepath"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/decrypt"
	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/s3"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
)

type CollectionMessage struct {
	CollectionId  string
	EncryptionKey string
	FileLocation  string
}

func uploadCollection(jsonMessage []byte) {
	var message CollectionMessage
	err := json.Unmarshal(jsonMessage, &message)
	if err != nil {
		log.Printf("Invalid json message received")
	} else if !strings.Contains(message.FileLocation, "data.json") {
		log.Printf("Uploading collectionId : %s", message.CollectionId)
		s3Client := s3.CreateS3Client()
		bucketName := utils.GetEnvironmentVariable("S3_BUCKET", "content")
		zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
		path := filepath.Join(zebedeeRoot, "collections", message.CollectionId, "complete", message.FileLocation)
		content, decryptErr := decrypt.DecryptFile(path, message.EncryptionKey)
		if decryptErr == nil {
			s3.AddFileToS3(s3Client, bucketName, string(content), message.FileLocation)
		} else {
			log.Printf("Failed to decrypt the following file : %s", path)
		}
	}
}

func main() {
	bucketName := utils.GetEnvironmentVariable("S3_BUCKET", "content")
	log.Printf("Starting Static Content Migrator")
	s3.SetupBucket(s3.CreateS3Client(), bucketName, "eu-west-1")
	master := kafka.CreateConsumer()
	defer func() {
		err := master.Close()
		if err != nil {
			panic(err)
		}
	}()
	topic := utils.GetEnvironmentVariable("TOPIC", "test")
	kafka.ProcessMessages(master, topic, uploadCollection)
	log.Printf("Service stopped")
}
