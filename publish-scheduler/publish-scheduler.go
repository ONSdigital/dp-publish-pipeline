package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strconv"
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

var TICK = time.Millisecond * 200
var sched sync.Mutex

func findCollectionFiles(zebedeeRoot, collectionId string) ([]string, error) {
	var files []string
	searchPath := filepath.Join(zebedeeRoot, "collections", collectionId, "complete")
	filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Panicf("Walk failed: %s", err.Error())
		}
		relPath, relErr := filepath.Rel(searchPath, path)
		if !info.IsDir() && relErr == nil {
			files = append(files, relPath)
		}
		return nil
	})
	return files, nil
}

func publishCollection(zebedeeRoot string, jsonMessage []byte, fileProducerChannel, totalProducerChannel chan []byte) {
	var message PublishMessage
	if err := json.Unmarshal(jsonMessage, &message); err != nil {
		log.Panicf("Failed to parse json: %v", jsonMessage)

	} else if message.CollectionId == "" {
		log.Panicf("Bad json: %s", jsonMessage)

	} else {
		var files []string
		var data []byte

		if files, err = findCollectionFiles(zebedeeRoot, message.CollectionId); err != nil {
			log.Panic(err)
		}
		for i := 0; i < len(files); i++ {
			if data, err = json.Marshal(PublishFileMessage{message.CollectionId, message.EncryptionKey, files[i]}); err != nil {
				log.Panic(err)
			}
			fileProducerChannel <- data
		}
		if data, err = json.Marshal(PublishTotalMessage{message.CollectionId, len(files)}); err != nil {
			log.Panic(err)
		}
		totalProducerChannel <- data
		log.Printf("Collection %q - sent %d files", message.CollectionId, len(files))

	}
}

func scheduleCollection(jsonMessage []byte, schedule *[]ScheduleMessage) {
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
		log.Printf("Collection %q schedule now len:%d %v", message.CollectionId, len(*schedule), *schedule)
		sched.Unlock()
	}
}

func checkSchedule(schedule *[]ScheduleMessage, producer chan []byte, publishChannel chan []byte) {
	epochTime := time.Now().Unix()
	sched.Lock()
	defer sched.Unlock()
	//log.Printf("%d check len:%d %v", epochTime, len(*schedule), schedule)
	for i := 0; i < len(*schedule); i++ {
		collectionId := (*schedule)[i].CollectionId
		if collectionId == "" {
			continue
		}
		scheduleTime, err := strconv.ParseInt((*schedule)[i].ScheduleTime, 10, 64)
		if err != nil {
			log.Panicf("Collection %q Cannot numeric convert: %q", collectionId, (*schedule)[i].ScheduleTime)
		}
		if scheduleTime <= epochTime {
			log.Printf("Collection %q found %d", collectionId, i)
			message := ScheduleMessage{CollectionId: collectionId, EncryptionKey: (*schedule)[i].EncryptionKey}
			(*schedule)[i] = ScheduleMessage{CollectionId: ""}
			jsonMessage, err := json.Marshal(message)
			if err != nil {
				log.Panicf("Marshal failed: %s", err)
			}
			publishChannel <- jsonMessage
		} else {
			log.Printf("Collection %q Not time for %d - %d > %d", collectionId, i, epochTime, scheduleTime)
		}
	}
}

func main() {
	zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	if fileinfo, err := os.Stat(zebedeeRoot); err != nil || fileinfo.IsDir() == false {
		log.Panicf("Cannot see directory %q", zebedeeRoot)
	}

	consumeTopic := utils.GetEnvironmentVariable("SCHEDULE_TOPIC", "uk.gov.ons.dp.web.schedule")
	produceFileTopic := utils.GetEnvironmentVariable("PUBLISH_FILE_TOPIC", "uk.gov.ons.dp.web.publish-file")
	produceTotalTopic := utils.GetEnvironmentVariable("PUBLISH_COUNT_TOPIC", "uk.gov.ons.dp.web.publish-count")

	schedule := make([]ScheduleMessage, 0, 10)

	log.Printf("Starting publish scheduler from %q topics: %q -> %q/%q", zebedeeRoot, consumeTopic, produceFileTopic, produceTotalTopic)

	totalProducer := kafka.NewProducer(produceTotalTopic)
	consumer := kafka.NewConsumer(consumeTopic)
	fileProducer := kafka.NewProducer(produceFileTopic)

	publishChannel := make(chan []byte, 10)
	exitChannel := make(chan bool)

	signals := make(chan os.Signal, 1)
	//signal.Notify(signals, os.Interrupt)

	go func() {
		for {
			select {
			case scheduleMessage := <-consumer.Incoming:
				scheduleCollection(scheduleMessage, &schedule)
			case publishMessage := <-publishChannel:
				publishCollection(zebedeeRoot, publishMessage, fileProducer.Output, totalProducer.Output)
			case <-time.After(TICK):
				checkSchedule(&schedule, totalProducer.Output, publishChannel)
			case <-signals:
				// log.Printf("Quitting publisher-scheduler of topic %q", consumeTopic)
				fileProducer.Closer <- true
				consumer.Closer <- true
				totalProducer.Closer <- true
				exitChannel <- true
				return
			}
		}
	}()
	<-exitChannel

	log.Printf("Service publish scheduler stopped")
}
