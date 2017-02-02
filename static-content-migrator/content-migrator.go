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

func uploadFile(zebedeeRoot string, jsonMessage []byte, bucketName string, completeFileProducer, completeFileFlagProducer kafka.Producer) {
	var message kafka.PublishFileMessage
	if err := json.Unmarshal(jsonMessage, &message); err != nil {
		log.Printf("Invalid json message received")
		return
	}
	if message.CollectionId == "" || message.EncryptionKey == "" || message.FileLocation == "" {
		log.Printf("Json message missing fields : %s", string(jsonMessage))
		return
	}
	if !strings.HasSuffix(message.FileLocation, ".json") {
		log.Printf("Collection %q - start %q", message.CollectionId, message.FileLocation)
		s3Client := s3.CreateClient(bucketName)
		path := filepath.Join(zebedeeRoot, "collections", message.CollectionId, "complete", message.FileLocation)
		content, decryptErr := decrypt.DecryptFile(path, message.EncryptionKey)
		if decryptErr == nil {
			s3Client.AddObject(string(content), message.FileLocation)
			s3Location := "s3://" + bucketName + message.FileLocation
			fileComplete, _ := json.Marshal(kafka.FileCompleteMessage{CollectionId: message.CollectionId, FileLocation: message.FileLocation, S3Location: s3Location})
			completeFileProducer.Output <- fileComplete
			fileComplete, _ = json.Marshal(kafka.FileCompleteFlagMessage{CollectionId: message.CollectionId, FileLocation: message.FileLocation})
			completeFileFlagProducer.Output <- fileComplete
		} else {
			log.Printf("Collection %q - Failed to decrypt the following file : %s", message.CollectionId, path)
		}
	}
}

func main() {
	zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	consumeTopic := utils.GetEnvironmentVariable("CONSUME_TOPIC", "uk.gov.ons.dp.web.publish-file")
	completeFileTopic := utils.GetEnvironmentVariable("PRODUCE_TOPIC", "uk.gov.ons.dp.web.complete-file")
	completeFileFlagTopic := utils.GetEnvironmentVariable("COMPLETE_FILE_FLAG_TOPIC", "uk.gov.ons.dp.web.complete-file-flag")
	bucketName := utils.GetEnvironmentVariable("S3_BUCKET", "content")
	regionName := utils.GetEnvironmentVariable("S3_REGION", "eu-west-1")
	log.Printf("Starting Static Content Migrator from %q from %q to %q, %q", zebedeeRoot, consumeTopic, completeFileTopic, completeFileFlagTopic)
	client := s3.CreateClient(bucketName)
	client.CreateBucket(regionName)
	consumer := kafka.NewConsumerGroup(consumeTopic, "content-migrator")
	completeFileProducer := kafka.NewProducer(completeFileTopic)
	completeFileFlagProducer := kafka.NewProducer(completeFileFlagTopic)
	for {
		select {
		case consumerMessage := <-consumer.Incoming:
			uploadFile(zebedeeRoot, consumerMessage.GetData(), bucketName, completeFileProducer, completeFileFlagProducer)
			consumerMessage.Commit()
		}
	}
}
