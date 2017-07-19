package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/decrypt"
	"github.com/ONSdigital/dp-publish-pipeline/health"
	"github.com/ONSdigital/dp-publish-pipeline/s3"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
)

func sendData(zebedeeRoot string, jsonMessage []byte, fileProducer, flagProducer kafka.Producer, s3UpstreamClient s3.S3Client) error {
	var message utils.PublishFileMessage
	err := json.Unmarshal(jsonMessage, &message)
	if err != nil {
		return fmt.Errorf("Failed to parse json message: %s", err)
	}
	if message.FileLocation == "" || message.CollectionId == "" || message.Uri == "" {
		return fmt.Errorf("Json message missing fields: %s", string(jsonMessage))
	}
	if !strings.HasSuffix(message.FileLocation, ".json") {
		return nil // leave non-metadata for other services
	}

	var content []byte
	var contentErr error
	if strings.HasPrefix(message.FileLocation, "file://") {
		if message.EncryptionKey != "" {
			content, contentErr = decrypt.DecryptFile(message.FileLocation[7:], message.EncryptionKey)
		} else {
			content, contentErr = ioutil.ReadFile(message.FileLocation[7:])
		}
	} else if strings.HasPrefix(message.FileLocation, "s3://") {
		bucketPrefix := "s3://" + s3UpstreamClient.Bucket + "/"
		if !strings.HasPrefix(message.FileLocation, bucketPrefix) {
			return fmt.Errorf("Unexpected bucket: wanted %s, for %s", bucketPrefix, message.FileLocation)
		}
		if message.EncryptionKey != "" {
			content, contentErr = decrypt.DecryptS3(s3UpstreamClient, message.FileLocation[len(bucketPrefix):], message.EncryptionKey)
		} else {
			content, contentErr = s3UpstreamClient.GetObject(message.FileLocation[len(bucketPrefix):])
		}
	} else {
		contentErr = fmt.Errorf("Bad FileLocation")
	}
	if contentErr != nil {
		return fmt.Errorf("Job %d Collection %q - Failed to obtain file %d: %q - %s", message.ScheduleId, message.CollectionId, message.FileId, message.FileLocation, contentErr)
	}

	data, _ := json.Marshal(utils.FileCompleteMessage{FileId: message.FileId, ScheduleId: message.ScheduleId, Uri: message.Uri, FileContent: string(content), CollectionId: message.CollectionId})
	fileProducer.Output() <- data
	data, _ = json.Marshal(utils.FileCompleteFlagMessage{FileId: message.FileId, ScheduleId: message.ScheduleId, Uri: message.Uri, CollectionId: message.CollectionId})
	flagProducer.Output() <- data

	log.Info(fmt.Sprintf("Job %d Collection %q - uri %s", message.ScheduleId, message.CollectionId, message.FileLocation), nil)
	return nil
}

func main() {
	log.Namespace = "publish-metadata"

	zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	brokers := utils.GetEnvironmentVariableAsArray("KAFKA_ADDR", "localhost:9092")
	consumeTopic := utils.GetEnvironmentVariable("CONSUME_TOPIC", "uk.gov.ons.dp.web.publish-file")
	completeFileTopic := utils.GetEnvironmentVariable("PRODUCE_TOPIC", "uk.gov.ons.dp.web.complete-file")
	completeFileFlagTopic := utils.GetEnvironmentVariable("COMPLETE_FILE_FLAG_TOPIC", "uk.gov.ons.dp.web.complete-file-flag")

	healthCheckAddr := utils.GetEnvironmentVariable("HEALTHCHECK_ADDR", ":8080")
	healthCheckEndpoint := utils.GetEnvironmentVariable("HEALTHCHECK_ENDPOINT", "/healthcheck")
	healthChannel := make(chan bool)

	upstreamBucketName := utils.GetEnvironmentVariable("UPSTREAM_S3_BUCKET", "upstream-content")
	upstreamRegionName := utils.GetEnvironmentVariable("UPSTREAM_S3_REGION", "eu-west-1")
	upstreamEndpoint := utils.GetEnvironmentVariable("UPSTREAM_S3_URL", "localhost:4000")
	IAM := (utils.GetEnvironmentVariable("UPSTREAM_S3_IAM", "1") == "1")
	s3Secure := (utils.GetEnvironmentVariable("UPSTREAM_S3_SECURE", "1") == "1")
	s3UpstreamClient, err := s3.CreateClient(upstreamRegionName, upstreamBucketName, upstreamEndpoint, IAM, s3Secure)
	if err != nil {
		log.ErrorC("Could not obtain s3 client", err, nil)
		panic(err)
	}

	log.Info(fmt.Sprintf("Starting Publish-metadata from %q to %q, %q", consumeTopic, completeFileTopic, completeFileFlagTopic), nil)
	consumer, err := kafka.NewConsumerGroup(brokers, consumeTopic, "publish-metadata", kafka.OffsetNewest)
	if err != nil {
		log.ErrorC("Could not obtain consumer", err, nil)
		panic(err)
	}
	fileProducer := kafka.NewProducer(brokers, completeFileTopic, 0)
	flagProducer := kafka.NewProducer(brokers, completeFileFlagTopic, 0)

	go func() {
		http.HandleFunc(healthCheckEndpoint, health.NewHealthChecker(healthChannel, nil))
		log.Info(fmt.Sprintf("Listening for %s on %s", healthCheckEndpoint, healthCheckAddr), nil)
		log.ErrorC("healthcheck listener exited", http.ListenAndServe(healthCheckAddr, nil), nil)
		panic("healthcheck listener exited")
	}()

	for {
		select {
		case consumerMessage := <-consumer.Incoming():
			if err := sendData(zebedeeRoot, consumerMessage.GetData(), fileProducer, flagProducer, s3UpstreamClient); err != nil {
				log.Error(err, nil)
			}
			consumerMessage.Commit()
		case errorMessage := <-consumer.Errors():
			log.Error(fmt.Errorf("Aborting: %s", errorMessage), nil)
			panic(errorMessage)
		case <-healthChannel:
		}
	}
}
