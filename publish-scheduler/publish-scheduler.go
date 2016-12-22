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
)

var tick = time.Millisecond * 200
var sched sync.Mutex

type scheduleJob struct {
	collectionId  string
	encryptionKey string
	scheduleTime  int64
	files         []string
}

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

func publishCollection(message scheduleJob, fileProducerChannel, totalProducerChannel chan []byte) {
	if message.collectionId == "" {
		log.Panicf("Bad message: %v", message)
	}

	var data []byte
	var err error

	if data, err = json.Marshal(kafka.PublishTotalMessage{message.collectionId, len(message.files)}); err != nil {
		log.Panic(err)
	}
	totalProducerChannel <- data

	for i := 0; i < len(message.files); i++ {
		if data, err = json.Marshal(kafka.PublishFileMessage{message.collectionId, message.encryptionKey, message.files[i]}); err != nil {
			log.Panic(err)
		}
		fileProducerChannel <- data
	}
	log.Printf("Collection %q - sent %d files", message.collectionId, len(message.files))
}

func scheduleCollection(jsonMessage []byte, schedule *[]scheduleJob, zebedeeRoot string) {
	var message kafka.ScheduleMessage
	if err := json.Unmarshal(jsonMessage, &message); err != nil {
		log.Panicf("Failed to parse json: %v", jsonMessage)
	} else if len(message.CollectionId) == 0 {
		log.Panicf("Empty collectionId: %v", jsonMessage)
	} else {
		scheduleTime, err := strconv.ParseInt(message.ScheduleTime, 10, 64)
		if err != nil {
			log.Panicf("Collection %q Cannot numeric convert: %q", message.CollectionId, message.ScheduleTime)
		}
		newJob := scheduleJob{collectionId: message.CollectionId, scheduleTime: scheduleTime, encryptionKey: message.EncryptionKey}

		addIndex := -1

		sched.Lock()
		defer sched.Unlock()

		for i := 0; i < len(*schedule); i++ {
			if (*schedule)[i].collectionId == "" {
				addIndex = i
				break
			}
		}

		var files []string
		if files, err = findCollectionFiles(zebedeeRoot, message.CollectionId); err != nil {
			log.Panic(err)
		}
		//log.Printf("coll %q found %d files", message.CollectionId, len(files))
		newJob.files = make([]string, len(files))
		copy(newJob.files, files)

		if addIndex == -1 {
			*schedule = append(*schedule, newJob)
		} else {
			(*schedule)[addIndex] = newJob
		}
		log.Printf("Collection %q schedule now len:%d files:%d", message.CollectionId, len(*schedule), len(newJob.files))
	}
}

func checkSchedule(schedule *[]scheduleJob, publishChannel chan scheduleJob) {
	epochTime := time.Now().Unix()
	sched.Lock()
	defer sched.Unlock()
	//log.Printf("%d check len:%d %v", epochTime, len(*schedule), schedule)
	for i := 0; i < len(*schedule); i++ {
		collectionId := (*schedule)[i].collectionId
		if collectionId == "" {
			continue
		}
		if (*schedule)[i].scheduleTime <= epochTime {
			log.Printf("Collection %q found %d", collectionId, i)
			// message := kafka.ScheduleMessage{CollectionId: collectionId, EncryptionKey: (*schedule)[i].encryptionKey}
			message := (*schedule)[i]
			(*schedule)[i] = scheduleJob{collectionId: ""}
			publishChannel <- message
		} else {
			log.Printf("Collection %q Not time for %d - %d > %d", collectionId, i, epochTime, (*schedule)[i].scheduleTime)
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

	schedule := make([]scheduleJob, 0, 10)

	log.Printf("Starting publish scheduler from %q topics: %q -> %q/%q", zebedeeRoot, consumeTopic, produceFileTopic, produceTotalTopic)

	totalProducer := kafka.NewProducer(produceTotalTopic)
	consumer := kafka.NewConsumer(consumeTopic)
	fileProducer := kafka.NewProducer(produceFileTopic)

	publishChannel := make(chan scheduleJob)
	exitChannel := make(chan bool)

	go func() {
		for {
			select {
			case scheduleMessage := <-consumer.Incoming:
				go scheduleCollection(scheduleMessage, &schedule, zebedeeRoot)
			case publishMessage := <-publishChannel:
				go publishCollection(publishMessage, fileProducer.Output, totalProducer.Output)
			case <-time.After(tick):
				go checkSchedule(&schedule, publishChannel)
			}
		}
	}()
	<-exitChannel

	log.Printf("Service publish scheduler stopped")
}
