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

func sendData(zebedeeRoot string, jsonMessage []byte, producer kafka.Producer) {
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
		file := filepath.Join(zebedeeRoot, "collections", message.CollectionId, "complete", message.FileLocation)
		content, decryptErr := decrypt.DecryptFile(file, message.EncryptionKey)
		if decryptErr != nil {
			log.Printf("Collection %q - Failed to decrypt the following file : %s", message.CollectionId, file)
			return
		}
		data, _ := json.Marshal(kafka.FileCompleteMessage{FileLocation: utils.GetUri(content), FileContent: string(content), CollectionId: message.CollectionId})
		producer.Output <- data
		log.Printf("Collection %q - uri %s", message.CollectionId, message.FileLocation)
	}
}

func main() {
	zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	consumeTopic := utils.GetEnvironmentVariable("CONSUME_TOPIC", "uk.gov.ons.dp.web.publish-file")
	produceTopic := utils.GetEnvironmentVariable("PRODUCE_TOPIC", "uk.gov.ons.dp.web.complete-file")
	log.Printf("Starting publish sender from %q to %q", consumeTopic, produceTopic)
	consumer := kafka.NewConsumerGroup(consumeTopic, "publish-sender")
	producer := kafka.NewProducer(produceTopic)
	for {
		select {
		case consumerMessage := <-consumer.Incoming:
			sendData(zebedeeRoot, consumerMessage.GetData(), producer)
			consumerMessage.Commit()
		}
	}
	// log.Println("publish sender stopped")
}
