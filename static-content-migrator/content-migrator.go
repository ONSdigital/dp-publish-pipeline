package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/decrypt"
	"github.com/ONSdigital/dp-publish-pipeline/s3"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/Shopify/sarama"
)

type CollectionMessage struct {
	CollectionId  string
	EncryptionKey string
}

func uploadCollection(jsonMessage []byte) {
	var message CollectionMessage
	err := json.Unmarshal(jsonMessage, &message)
	if err != nil {
		log.Printf("Invalid json message received")
	} else {
		log.Printf("Uploading collectionId : %s", message.CollectionId)
		files := findFiles(message.CollectionId)
		s3Client := s3.CreateS3Client()
		bucketName := utils.GetEnvironmentVariable("S3_BUCKET", "static-content")
		for i := 0; i < len(files); i++ {
			file := files[i]
			log.Printf("decrypt file %s", file)
			content := decrypt.DecryptFile(file, message.EncryptionKey)
			// Split the string just after the <collectionId>/complete, this creates the
			// url needed within the secound element of the array.
			url := strings.Split(file, message.CollectionId+"/complete")[1]
			s3.AddFileToS3(s3Client, bucketName, string(content), url)
		}
	}
}

func findFiles(collectionId string) []string {
	zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	searchPath := zebedeeRoot + "collections/" + collectionId + "/complete/"
	var files []string
	filepath.Walk(searchPath, func(path string, _ os.FileInfo, _ error) error {
		base := filepath.Base(path)
		if strings.Contains(base, ".png") {
			files = append(files, path)
		}
		return nil
	})
	return files
}

func processBusMessages(master sarama.Consumer) {
	topic := utils.GetEnvironmentVariable("TOPIC", "test")

	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	messageChannel := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				log.Printf("Error : %s", err.Error())
			case msg := <-consumer.Messages():
				uploadCollection(msg.Value)
			case <-signals:
				log.Printf("Closing down service")
				messageChannel <- struct{}{}
			}
		}
	}()
	<-messageChannel
}

func createConsumer() sarama.Consumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	brokers := []string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}

	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}
	return master
}

func main() {
	bucketName := utils.GetEnvironmentVariable("S3_BUCKET", "static-content")
	log.Printf("Starting Static Content Migrator")
	s3.SetupBucket(s3.CreateS3Client(), bucketName, "eu-west-1")
	master := createConsumer()
	defer func() {
		err := master.Close()
		if err != nil {
			panic(err)
		}
	}()
	processBusMessages(master)
	log.Printf("Service stopped")
}
