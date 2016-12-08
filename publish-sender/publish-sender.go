package main

import (
	"encoding/json"
	"log"
	"os"
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
	} else {
		log.Printf("Uploading collectionId : %s", message.CollectionId)
		files := findDatailes(message.CollectionId)
		for i := 0; i < len(files); i++ {
			file := files[i]
			content, _ := decrypt.DecryptFile(file, message.EncryptionKey)
			// Split the string just after the <collectionId>/complete, this creates the
			// url needed within the secound element of the array.
			url := strings.Split(file, message.CollectionId+"/complete")[1]

			data, _ := json.Marshal(DataSet{url, string(content), message.CollectionId})
			message := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(data)}
			producer.Input() <- message
			log.Printf("Sent %s", url)
		}
	}
}

func findDatailes(collectionId string) []string {
	zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	searchPath := filepath.Join(zebedeeRoot, "collections", collectionId, "complete")
	var files []string
	filepath.Walk(searchPath, func(path string, info os.FileInfo, _ error) error {
		base := filepath.Base(path)
		if strings.Contains(base, "data.json") && !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files
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
	consumeTopic := utils.GetEnvironmentVariable("CONSUME_TOPIC", "test")
	produceTopic := utils.GetEnvironmentVariable("PRODUCE_TOPIC", "test")
	kafka.ConsumeAndProduceMessages(master, consumeTopic, produceTopic, sendData)
	log.Printf("Service stopped")
}
