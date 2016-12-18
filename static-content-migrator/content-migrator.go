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

type PublishFile struct {
	CollectionId  string `json:"collectionId"`
	EncryptionKey string `json:"encryptionKey"`
	FileLocation  string `json:"fileLocation"`
}

type FileComplete struct {
	CollectionId string `json:"collectionId"`
	FileLocation string `json:"fileLocation"`
	S3Location   string `json:"s3Location"`
}

func uploadFile(zebedeeRoot string, jsonMessage []byte, bucketName string, producer kafka.Producer) {
	var message PublishFile
	if err := json.Unmarshal(jsonMessage, &message); err != nil {
		log.Printf("Invalid json message received")
		return
	}
	if message.CollectionId == "" || message.EncryptionKey == "" || message.FileLocation == "" {
		log.Printf("Json message missing fields : %s", string(jsonMessage))
		return
	}
	if !strings.Contains(message.FileLocation, "data.json") {
		log.Printf("Uploading collectionId : %s", message.CollectionId)
		s3Client := s3.CreateS3Client()
		path := filepath.Join(zebedeeRoot, "collections", message.CollectionId, "complete", message.FileLocation)
		content, decryptErr := decrypt.DecryptFile(path, message.EncryptionKey)
		if decryptErr == nil {
			s3.AddFileToS3(s3Client, bucketName, string(content), message.FileLocation)
			s3Location := "s3://" + bucketName + message.FileLocation
			fileComplete, _ := json.Marshal(FileComplete{message.CollectionId, message.FileLocation, s3Location})
			producer.Output <- fileComplete
		} else {
			log.Printf("Failed to decrypt the following file : %s", path)
		}
	}
}

func main() {
	zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	consumeTopic := utils.GetEnvironmentVariable("CONSUME_TOPIC", "uk.gov.ons.dp.web.publish-file")
	produceTopic := utils.GetEnvironmentVariable("PRODUCE_TOPIC", "uk.gov.ons.dp.web.complete-file")
	bucketName := utils.GetEnvironmentVariable("S3_BUCKET", "content")
	regionName := utils.GetEnvironmentVariable("S3_REGION", "eu-west-1")
	log.Printf("Starting Static Content Migrator from %q from %q to %q", zebedeeRoot, consumeTopic, produceTopic)
	s3.SetupBucket(s3.CreateS3Client(), bucketName, regionName)
	consumer := kafka.NewConsumer(consumeTopic)
	producer := kafka.NewProducer(produceTopic)
	for {
		select {
		case consumerMessage := <-consumer.Incoming:
			go uploadFile(zebedeeRoot, consumerMessage, bucketName, producer)
		}
	}
	//log.Printf("Service stopped")
}
