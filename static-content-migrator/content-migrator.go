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
	"github.com/Shopify/sarama"
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

func uploadFile(jsonMessage []byte, producer sarama.AsyncProducer, topic string) {
	var message PublishFile
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
			s3Location := "s3://" + bucketName + message.FileLocation
			fileComplete, _ := json.Marshal(FileComplete{message.CollectionId, message.FileLocation, s3Location})
			producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(fileComplete)}
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
	consumeTopic := utils.GetEnvironmentVariable("CONSUME_TOPIC", "uk.gov.ons.dp.web.publish-file")
	produceTopic := utils.GetEnvironmentVariable("PRODUCE_TOPIC", "uk.gov.ons.dp.web.complete-file")
	kafka.ConsumeAndProduceMessages(master, consumeTopic, produceTopic, uploadFile)
	log.Printf("Service stopped")
}
