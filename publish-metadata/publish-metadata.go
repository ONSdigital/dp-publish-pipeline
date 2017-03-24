package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/decrypt"
	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/s3"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
)

func sendData(zebedeeRoot string, jsonMessage []byte, fileProducer, flagProducer kafka.Producer, s3UpstreamClient s3.S3Client) error {
	var message kafka.PublishFileMessage
	err := json.Unmarshal(jsonMessage, &message)
	if err != nil {
		return fmt.Errorf("Failed to parse json message: %s", err)
	}
	if message.FileLocation == "" || message.EncryptionKey == "" || message.CollectionId == "" || message.Uri == "" {
		return fmt.Errorf("Json message missing fields: %s", string(jsonMessage))
	}
	if !strings.HasSuffix(message.FileLocation, ".json") {
		return nil // leave non-metadata for other services
	}

	var content []byte
	var decryptErr error
	if strings.HasPrefix(message.FileLocation, "file://") {
		content, decryptErr = decrypt.DecryptFile(message.FileLocation[7:], message.EncryptionKey)
	} else if strings.HasPrefix(message.FileLocation, "s3://") {
		bucketPrefix := "s3://" + s3UpstreamClient.Bucket + "/"
		if !strings.HasPrefix(message.FileLocation, bucketPrefix) {
			return fmt.Errorf("Unexpected bucket: wanted %s, for %s", bucketPrefix, message.FileLocation)
		}
		content, decryptErr = decrypt.DecryptS3(s3UpstreamClient, message.FileLocation[len(bucketPrefix):], message.EncryptionKey)
	} else {
		decryptErr = fmt.Errorf("Bad FileLocation")
	}
	if decryptErr != nil {
		return fmt.Errorf("Job %d Collection %q - Failed to decrypt file %d: %q - %s", message.ScheduleId, message.CollectionId, message.FileId, message.FileLocation, decryptErr)
	}

	data, _ := json.Marshal(kafka.FileCompleteMessage{FileId: message.FileId, ScheduleId: message.ScheduleId, Uri: message.Uri, FileContent: string(content), CollectionId: message.CollectionId})
	fileProducer.Output <- data
	data, _ = json.Marshal(kafka.FileCompleteFlagMessage{FileId: message.FileId, ScheduleId: message.ScheduleId, Uri: message.Uri, CollectionId: message.CollectionId})
	flagProducer.Output <- data

	log.Printf("Job %d Collection %q - uri %s", message.ScheduleId, message.CollectionId, message.FileLocation)
	return nil
}

func main() {
	zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	consumeTopic := utils.GetEnvironmentVariable("CONSUME_TOPIC", "uk.gov.ons.dp.web.publish-file")
	completeFileTopic := utils.GetEnvironmentVariable("PRODUCE_TOPIC", "uk.gov.ons.dp.web.complete-file")
	completeFileFlagTopic := utils.GetEnvironmentVariable("COMPLETE_FILE_FLAG_TOPIC", "uk.gov.ons.dp.web.complete-file-flag")

	upstreamBucketName := utils.GetEnvironmentVariable("UPSTREAM_S3_BUCKET", "upstream-content")
	//upstreamRegionName := utils.GetEnvironmentVariable("UPSTREAM_S3_REGION", "eu-west-2")
	upstreamEndpoint := utils.GetEnvironmentVariable("UPSTREAM_S3_URL", "localhost:4000")
	upstreamAccessKeyID := utils.GetEnvironmentVariable("UPSTREAM_S3_ACCESS_KEY", "1234")
	upstreamSecretAccessKey := utils.GetEnvironmentVariable("UPSTREAM_S3_SECRET_ACCESS_KEY", "1234")
	s3UpstreamClient := s3.CreateClient(upstreamBucketName, upstreamEndpoint, upstreamAccessKeyID, upstreamSecretAccessKey, false)

	log.Printf("Starting Publish-metadata from %q to %q, %q", consumeTopic, completeFileTopic, completeFileFlagTopic)
	consumer := kafka.NewConsumerGroup(consumeTopic, "publish-metadata")
	fileProducer := kafka.NewProducer(completeFileTopic)
	flagProducer := kafka.NewProducer(completeFileFlagTopic)
	for {
		select {
		case consumerMessage := <-consumer.Incoming:
			if err := sendData(zebedeeRoot, consumerMessage.GetData(), fileProducer, flagProducer, s3UpstreamClient); err != nil {
				log.Printf("Error: %s", err)
			}
			consumerMessage.Commit()
		}
	}
}
