package main

import (
	"encoding/json"
	"log"
	"path/filepath"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/decrypt"
	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/Shopify/sarama"
)

type CollectionMessage struct {
	CollectionId  string
	EncryptionKey string
	FileLocation  string
}

type DataSet struct {
	FileLocation string
	FileContent  string
	CollectionId string
}

func sendData(jsonMessage []byte, producer sarama.AsyncProducer, topic string) {
	var message CollectionMessage
	err := json.Unmarshal(jsonMessage, &message)
	if err != nil {
		log.Printf("Failed to parse json message")
	} else if message.FileLocation != "" && message.EncryptionKey != "" && message.CollectionId != "" {
		if strings.Contains(message.FileLocation, "data.json") {
			zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
			file := filepath.Join(zebedeeRoot, "collections", message.CollectionId, "complete", message.FileLocation)
			content, decryptErr := decrypt.DecryptFile(file, message.EncryptionKey)
			if decryptErr != nil {
				log.Printf("Failed to decrypt the following file : %s", file)
			} else {
				data, _ := json.Marshal(DataSet{message.FileLocation, string(content), message.CollectionId})
				producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(data)}
				log.Printf("Sent %s", message.FileLocation)
			}
		}
	} else {
		log.Printf("Json message missing fields : %s", string(jsonMessage))
	}
}

func main() {
	log.Printf("Starting publish sender")
	master := kafka.CreateConsumer()
	defer func() {
		err := master.Close()
		if err != nil {
			panic(err)
		}
	}()
	consumeTopic := utils.GetEnvironmentVariable("CONSUME_TOPIC", "uk.gov.ons.dp.web.publish-file")
	produceTopic := utils.GetEnvironmentVariable("PRODUCE_TOPIC", "uk.gov.ons.dp.web.complete-file")
	kafka.ConsumeAndProduceMessages(master, consumeTopic, produceTopic, sendData)
	log.Printf("Service stopped")
}
