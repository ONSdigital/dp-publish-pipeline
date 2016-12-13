package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/Shopify/sarama"
)

type ScheduleMessage struct {
	CollectionId  string
	EncryptionKey string
	ScheduleTime  string
	Producer      sarama.AsyncProducer `json:"-"`
}

type PublishMessage struct {
	CollectionId  string
	EncryptionKey string
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

var zebedeeRoot, produceFileTopic, produceTotalTopic string
var totalProducer sarama.AsyncProducer
var publishChannel chan kafka.ConsumeMessage
var sched sync.Mutex

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

func publishCollection(jsonMessage []byte, producer sarama.AsyncProducer) {
	var message PublishMessage
	if err := json.Unmarshal(jsonMessage, &message); err != nil {
		log.Panicf("Failed to parse json: %v", jsonMessage)

	} else if message.CollectionId == "" {
		log.Panicf("Bad json: %s", jsonMessage)

	} else {
		var files []string
		var data []byte

		if files, err = findCollectionFiles(message.CollectionId); err != nil {
			log.Panic(err)
		}
		for i := 0; i < len(files); i++ {
			if data, err = json.Marshal(PublishFileMessage{message.CollectionId, message.EncryptionKey, files[i]}); err != nil {
				log.Panic(err)
			}
			producer.Input() <- &sarama.ProducerMessage{Topic: produceFileTopic, Value: sarama.StringEncoder(data)}
		}
		if data, err = json.Marshal(PublishTotalMessage{message.CollectionId, len(files)}); err != nil {
			log.Panic(err)
		}
		totalProducer.Input() <- &sarama.ProducerMessage{Topic: produceTotalTopic, Value: sarama.StringEncoder(data)}
		log.Printf("Sent collection '%s' - %d files", message.CollectionId, len(files))

	}
}

func scheduleCollection(jsonMessage []byte, producer sarama.AsyncProducer, schedule *[]ScheduleMessage) {
	var message ScheduleMessage
	if err := json.Unmarshal(jsonMessage, &message); err != nil {
		log.Panicf("Failed to parse json: %v", jsonMessage)
	} else if len(message.CollectionId) == 0 {
		log.Panicf("Empty collectionId: %v", jsonMessage)
	} else {
		addIndex := -1
		sched.Lock()
		for i := 0; i < len(*schedule); i++ {
			if (*schedule)[i].CollectionId == "" {
				addIndex = i
				break
			}
		}
		if addIndex == -1 {
			*schedule = append(*schedule, message)
		} else {
			(*schedule)[addIndex] = message
		}
		log.Printf("schedule now len:%d %v", len(*schedule), *schedule)
		sched.Unlock()
	}
}

func checkSchedule(schedule *[]ScheduleMessage, producer sarama.AsyncProducer) {
	doSchedule := -1
	sched.Lock()
	log.Printf("search len:%d %v", len(*schedule), schedule)
	for i := 0; i < len(*schedule); i++ {
		if (*schedule)[i].CollectionId != "" {
			log.Printf("found %d id: %s", i, (*schedule)[i].CollectionId)
			doSchedule = i
			break
		}
	}
	var message ScheduleMessage
	if doSchedule != -1 {
		message = ScheduleMessage{CollectionId: (*schedule)[doSchedule].CollectionId, EncryptionKey: (*schedule)[doSchedule].EncryptionKey}
		(*schedule)[doSchedule] = ScheduleMessage{CollectionId: ""}
	}
	sched.Unlock()

	if doSchedule != -1 {
		jsonMessage, err := json.Marshal(message)
		if err != nil {
			log.Panicf("Marshal failed: %s", err)
		}
		publishChannel <- kafka.ConsumeMessage{Payload: jsonMessage, Producer: producer}
	}
}

func main() {
	zebedeeRoot = utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	if fileinfo, err := os.Stat(zebedeeRoot); err != nil || fileinfo.IsDir() == false {
		log.Panicf("Cannot see directory '%s'", zebedeeRoot)
	}
	consumeTopic := utils.GetEnvironmentVariable("CONSUME_TOPIC", "test")
	produceFileTopic = utils.GetEnvironmentVariable("PRODUCE_TOPIC", "test")
	produceTotalTopic = utils.GetEnvironmentVariable("TOTAL_TOPIC", "test")
	schedule := make([]ScheduleMessage, 0, 10)

	log.Printf("Starting publish scheduler from '%s' topics: '%s' -> '%s'/'%s'", zebedeeRoot, consumeTopic, produceFileTopic, produceTotalTopic)

	var err error
	totalProducer, err = sarama.NewAsyncProducer([]string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}, nil)
	if err != nil {
		log.Panic(err)
	}

	consumer := kafka.CreateConsumer()
	if consumer == nil {
		log.Panicf("Cannot create consumer for '%s'", consumeTopic)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Panic(err)
		}
	}()

	scheduleChannel := make(chan kafka.ConsumeMessage, 10)
	publishChannel = make(chan kafka.ConsumeMessage, 10)
	exitChannel := make(chan bool)
	go kafka.ConsumeAndChannelMessages(consumer, consumeTopic, produceFileTopic, scheduleChannel)

	go func() {
	MainLoop:
		for {
			select {
			case scheduleMessage := <-scheduleChannel:
				if scheduleMessage.Payload == nil {
					break MainLoop
				}
				if scheduleMessage.Producer == nil {
					exitChannel <- false
					panic("Schedule Producer is nil")
				}
				scheduleCollection(scheduleMessage.Payload, scheduleMessage.Producer, &schedule)

			case publishMessage := <-publishChannel:
				if publishMessage.Payload == nil {
					break MainLoop
				}
				if publishMessage.Producer == nil {
					exitChannel <- false
					panic("Publish Producer is nil")
				}
				publishCollection(publishMessage.Payload, publishMessage.Producer)

			case <-time.After(time.Millisecond * 1200):
				checkSchedule(&schedule, totalProducer)
			}
		}
		exitChannel <- false
	}()
	<-exitChannel

	if err := totalProducer.Close(); err != nil {
		log.Fatalln(err)
	}
	log.Printf("Service publish scheduler stopped")
}
