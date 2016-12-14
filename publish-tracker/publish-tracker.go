package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
)

type NewCollectionRelease struct {
	CollectionId string
	FileCount    int
}

type FileComplete struct {
	CollectionId string
	FileLocation string
}

type CollectionComplete struct {
	CollectionId string
}

type Tracker struct {
	CollectionId string
	Total        int
	Files        []string
}

func (t *Tracker) Update(file FileComplete) bool {
	t.Files = append(t.Files, file.FileLocation)
	log.Printf("CollectionId : %s - Progress %d/%d", t.CollectionId, len(t.Files), t.Total)
	return t.IsFinished()
}

func (t *Tracker) IsFinished() bool {
	if len(t.Files) == t.Total {
		log.Printf("CollectionId : %s - Completed %d/%d", t.CollectionId, len(t.Files), t.Total)
		return true
	} else {
		return false
	}
}

// This should be locked with a mutex as mutliple go routines access the map
var releases map[string]Tracker = make(map[string]Tracker)

func trackNewRelease(jsonMessage []byte, producer kafka.ProducerInterface) {
	var newCollection NewCollectionRelease
	err := json.Unmarshal(jsonMessage, &newCollection)
	if err != nil {
		log.Printf("Failed to parse json message")
	} else if newCollection.CollectionId != "" && newCollection.FileCount != 0 {
		tracker, exists := releases[newCollection.CollectionId]

		// There a change the messages between the two consumers can be out of sync
		// so we need to check if it exists first.
		if !exists {
			tracker = Tracker{newCollection.CollectionId, newCollection.FileCount, make([]string, 0)}
		} else {
			// Else update the current tracker with the total count and key
			tracker.Total = newCollection.FileCount
		}

		if tracker.IsFinished() == true {
			data, _ := json.Marshal(CollectionComplete{tracker.CollectionId})
			producer.SendMessage(data)
			delete(releases, newCollection.CollectionId)
		} else {
			releases[newCollection.CollectionId] = tracker
		}
	} else {
		log.Printf("Json message is mising fields : %s", string(jsonMessage))
	}
}

func trackInprogressRelease(jsonMessage []byte, producer kafka.ProducerInterface) {
	var file FileComplete
	err := json.Unmarshal(jsonMessage, &file)
	if err != nil {
		log.Printf("Failed to parse json message")
	} else if file.CollectionId != "" && file.FileLocation != "" {
		tracker, exists := releases[file.CollectionId]
		if !exists {
			// Recieved an event for collection that not had it tracker started,
			// so create it as we can start storing the files published.
			tracker = Tracker{file.CollectionId, 0, make([]string, 0)}
		}
		completed := tracker.Update(file)
		if completed {
			data, _ := json.Marshal(CollectionComplete{tracker.CollectionId})
			producer.SendMessage(data)
			delete(releases, file.CollectionId)
		} else {
			releases[file.CollectionId] = tracker
		}
	} else {
		log.Printf("Json message is mising fields : %s", string(jsonMessage))
	}
}

func main() {
	log.Printf("Starting publish tracker")
	publishCountTopic := utils.GetEnvironmentVariable("PUBLISH_COUNT_TOPIC", "uk.gov.ons.dp.web.publish-count")
	competeFileTopic := utils.GetEnvironmentVariable("COMPLETE_FILE_TOPIC", "uk.gov.ons.dp.web.complete-file")
	competeTopic := utils.GetEnvironmentVariable("COMPLETE_TOPIC", "uk.gov.ons.dp.web.complete")
	master := kafka.CreateConsumer()
	defer func() {
		err := master.Close()
		if err != nil {
			panic(err)
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	publishCountChannel := make(chan []byte, 10)
	competeFileChannel := make(chan []byte, 10)
	kafka.ProcessMessages(master, publishCountTopic, publishCountChannel)
	kafka.ProcessMessages(master, competeFileTopic, competeFileChannel)
	producer := kafka.CreateProducer(competeTopic)
	defer producer.Close()

	for {
		select {
		case msg := <-publishCountChannel:
			trackNewRelease(msg, producer)
		case msg := <-competeFileChannel:
			trackInprogressRelease(msg, producer)
		case <-signals:
			log.Printf("Stopped publish tracker")
			return
		}
	}
}
