package main

import (
	"encoding/json"
	"log"
	"path/filepath"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/decrypt"
	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
)

func sendData(zebedeeRoot string, jsonMessage []byte, fileProducer, flagProducer kafka.Producer) {
	var message kafka.PublishFileMessage
	err := json.Unmarshal(jsonMessage, &message)
	if err != nil {
		log.Printf("Failed to parse json message")
		return
	}
	if message.FileLocation == "" || message.EncryptionKey == "" || message.CollectionId == "" {
		log.Printf("Json message missing fields: %s", string(jsonMessage))
		return
	}
	if strings.HasSuffix(message.FileLocation, ".json") {
		file := filepath.Join(zebedeeRoot, "collections", message.CollectionPath, "complete", message.FileLocation)
		content, decryptErr := decrypt.DecryptFile(file, message.EncryptionKey)
		if decryptErr != nil {
			log.Printf("Job %d Collection %q - Failed to decrypt file %d: %q", message.ScheduleId, message.CollectionId, message.FileId, file)
			return
		}

		data, _ := json.Marshal(kafka.FileCompleteMessage{FileId: message.FileId, ScheduleId: message.ScheduleId, FileLocation: message.FileLocation, FileContent: string(content), CollectionId: message.CollectionId})
		fileProducer.Output <- data
		data, _ = json.Marshal(kafka.FileCompleteFlagMessage{FileId: message.FileId, ScheduleId: message.ScheduleId, FileLocation: message.FileLocation, CollectionId: message.CollectionId})
		flagProducer.Output <- data
		log.Printf("Job %d Collection %q - uri %s", message.ScheduleId, message.CollectionId, message.FileLocation)
	}
}

func main() {
	zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	consumeTopic := utils.GetEnvironmentVariable("CONSUME_TOPIC", "uk.gov.ons.dp.web.publish-file")
	completeFileTopic := utils.GetEnvironmentVariable("PRODUCE_TOPIC", "uk.gov.ons.dp.web.complete-file")
	completeFileFlagTopic := utils.GetEnvironmentVariable("COMPLETE_FILE_FLAG_TOPIC", "uk.gov.ons.dp.web.complete-file-flag")

	log.Printf("Starting Publish-metadata from %q to %q, %q", consumeTopic, completeFileTopic, completeFileFlagTopic)
	consumer := kafka.NewConsumerGroup(consumeTopic, "publish-metadata")
	fileProducer := kafka.NewProducer(completeFileTopic)
	flagProducer := kafka.NewProducer(completeFileFlagTopic)
	for {
		select {
		case consumerMessage := <-consumer.Incoming:
			sendData(zebedeeRoot, consumerMessage.GetData(), fileProducer, flagProducer)
			consumerMessage.Commit()
		}
	}
}