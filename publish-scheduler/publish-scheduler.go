package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/Shopify/sarama"
)

type ScheduleMessage struct {
	CollectionId  string
	EncryptionKey string
	ScheduleTime  string
}

type PublishFileMessage struct {
	CollectionId  string
	EncryptionKey string
	FileLocation  string
}

type PublishTotalMessage struct {
	CollectionId string
	FileCount    int
}

var zebedeeRoot, produceTotalTopic string
var totalProducer sarama.AsyncProducer

func findCollectionFiles(collectionId string) ([]string, error) {
	var files []string
	searchPath := filepath.Join(zebedeeRoot, "collections", collectionId, "complete")
	filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Panicf("Walk failed: %s", err.Error())
		}
		// base := filepath.Base(path)
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files, nil
}

func publishCollection(message ScheduleMessage, producer sarama.AsyncProducer, topic string) {
	var data []byte
	var err error

	if len(message.CollectionId) > 0 {
		var files []string
		if files, err = findCollectionFiles(message.CollectionId); err != nil {
			log.Panic(err)
		}
		for i := 0; i < len(files); i++ {
			if data, err = json.Marshal(PublishFileMessage{message.CollectionId, message.EncryptionKey, files[i]}); err != nil {
				log.Panic(err)
			}
			producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(data)}
		}
		if data, err = json.Marshal(PublishTotalMessage{message.CollectionId, len(files)}); err != nil {
			log.Panic(err)
		}
		totalProducer.Input() <- &sarama.ProducerMessage{Topic: produceTotalTopic, Value: sarama.StringEncoder(data)}
		log.Printf("Sent collection '%s' - %d files", message.CollectionId, len(files))

	}
}

func scheduleCollection(jsonMessage []byte, producer sarama.AsyncProducer, topic string) {
	var message ScheduleMessage
	if err := json.Unmarshal(jsonMessage, &message); err != nil {
		log.Printf("Failed to parse json message")
	} else if len(message.CollectionId) == 0 {
		log.Panic("Empty collectionId")
	} else {
		publishCollection(message, producer, topic)
	}
}

func main() {
	zebedeeRoot = utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	if fileinfo, err := os.Stat(zebedeeRoot); err != nil || fileinfo.IsDir() == false {
		log.Panicf("Cannot see directory '%s'", zebedeeRoot)
	}
	consumeTopic := utils.GetEnvironmentVariable("CONSUME_TOPIC", "test")
	produceFileTopic := utils.GetEnvironmentVariable("PRODUCE_TOPIC", "test")
	produceTotalTopic = utils.GetEnvironmentVariable("TOTAL_TOPIC", "test")

	log.Printf("Starting publish scheduler from '%s' topics: '%s' -> '%s'/'%s'", zebedeeRoot, consumeTopic, produceFileTopic, produceTotalTopic)

	var err error
	totalProducer, err = sarama.NewAsyncProducer([]string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}, nil)
	if err != nil {
		log.Panic(err)
	}

	master := kafka.CreateConsumer()
	if master == nil {
		log.Panicf("Cannot create consumer for '%s'", consumeTopic)
	}
	defer func() {
		if err := master.Close(); err != nil {
			log.Panic(err)
		}
	}()
	kafka.ConsumeAndProduceMessages(master, consumeTopic, produceFileTopic, scheduleCollection)
	if err := totalProducer.Close(); err != nil {
		log.Fatalln(err)
	}
	log.Printf("Service publish scheduler stopped")
}
