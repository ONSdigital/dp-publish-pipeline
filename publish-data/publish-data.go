package main

import (
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/decrypt"
	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/s3"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	uuid "github.com/satori/go.uuid"
)

func uploadFile(zebedeeRoot string, jsonMessage []byte, s3UpstreamClient, s3Client s3.S3Client, completeFileProducer, completeFileFlagProducer kafka.Producer) error {
	var message kafka.PublishFileMessage
	if err := json.Unmarshal(jsonMessage, &message); err != nil {
		return fmt.Errorf("Invalid JSON: %q", jsonMessage)
	}
	if message.CollectionId == "" || message.EncryptionKey == "" || message.FileLocation == "" {
		return fmt.Errorf("Malformed JSON: %q", jsonMessage)
	}
	if strings.HasSuffix(message.FileLocation, ".json") {
		return nil
	}
	var content []byte
	var decryptErr error
	if strings.HasPrefix(message.FileLocation, "s3://") {
		bucketPrefix := "s3://" + s3UpstreamClient.Bucket + "/"
		if !strings.HasPrefix(message.FileLocation, bucketPrefix) {
			return fmt.Errorf("Unexpected bucket: wanted %s, for %s", bucketPrefix, message.FileLocation)
		}
		content, decryptErr = decrypt.DecryptS3(s3UpstreamClient, message.FileLocation[len(bucketPrefix):], message.EncryptionKey)
	} else if strings.HasPrefix(message.FileLocation, "file://") {
		content, decryptErr = decrypt.DecryptFile(message.FileLocation[7:], message.EncryptionKey)
	} else {
		decryptErr = fmt.Errorf("Bad URI")
	}
	if decryptErr != nil {
		return fmt.Errorf("Job %d Collection %q - Failed to decrypt file %d: %s - error %s", message.ScheduleId, message.CollectionId, message.FileId, message.FileLocation, decryptErr)
	}
	s3Path := filepath.Join(uuid.NewV1().String(), message.CollectionPath, filepath.Base(message.FileLocation))
	s3Client.AddObject(string(content), s3Path, message.CollectionId, message.ScheduleId)
	fullS3Path := "s3://" + s3Client.Bucket + "/" + s3Path
	fileComplete, _ := json.Marshal(kafka.FileCompleteMessage{FileId: message.FileId, ScheduleId: message.ScheduleId, CollectionId: message.CollectionId, FileLocation: message.FileLocation, S3Location: fullS3Path})
	completeFileProducer.Output <- fileComplete
	fileComplete, _ = json.Marshal(kafka.FileCompleteFlagMessage{FileId: message.FileId, ScheduleId: message.ScheduleId, CollectionId: message.CollectionId, FileLocation: message.FileLocation})
	completeFileFlagProducer.Output <- fileComplete

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

	bucketName := utils.GetEnvironmentVariable("S3_BUCKET", "content")
	regionName := utils.GetEnvironmentVariable("S3_REGION", "eu-west-1")
	endpoint := utils.GetEnvironmentVariable("S3_URL", "localhost:4000")
	accessKeyID := utils.GetEnvironmentVariable("S3_ACCESS_KEY", "1234")
	secretAccessKey := utils.GetEnvironmentVariable("S3_SECRET_ACCESS_KEY", "1234")
	s3Client := s3.CreateClient(bucketName, endpoint, accessKeyID, secretAccessKey, false)
	s3Client.CreateBucket(regionName)

	log.Printf("Starting Publish-Data from %q from %q to %q, %q", zebedeeRoot, consumeTopic, completeFileTopic, completeFileFlagTopic)

	consumer := kafka.NewConsumerGroup(consumeTopic, "publish-data")
	completeFileProducer := kafka.NewProducer(completeFileTopic)
	completeFileFlagProducer := kafka.NewProducer(completeFileFlagTopic)
	for {
		select {
		case consumerMessage := <-consumer.Incoming:
			if err := uploadFile(zebedeeRoot, consumerMessage.GetData(), s3UpstreamClient, s3Client, completeFileProducer, completeFileFlagProducer); err != nil {
				log.Print(err)
			} else {
				consumerMessage.Commit()
			}
		}
	}
}
