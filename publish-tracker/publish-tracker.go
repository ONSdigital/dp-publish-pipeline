package main

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
)

type Tracker struct {
	CollectionId string
	Total        int
	Files        []string
}

func (t *Tracker) Update(file kafka.FileCompleteMessage) bool {
	t.Files = append(t.Files, file.FileLocation)
	log.Printf("Collection %q - Progress %d/%d", t.CollectionId, len(t.Files), t.Total)
	return t.IsFinished()
}

func (t *Tracker) IsFinished() bool {
	if len(t.Files) == t.Total {
		log.Printf("Collection %q - Completed %d/%d", t.CollectionId, len(t.Files), t.Total)
		return true
	} else {
		return false
	}
}

// This should be locked with a mutex as mutliple go routines access the map
var releases map[string]Tracker = make(map[string]Tracker)
var mutex sync.Mutex

func trackNewRelease(jsonMessage []byte, producer kafka.Producer) {
	var newCollection kafka.PublishTotalMessage
	err := json.Unmarshal(jsonMessage, &newCollection)
	if err != nil {
		log.Printf("Failed to parse json message")
		return
	}
	if newCollection.CollectionId == "" || newCollection.FileCount == 0 {
		log.Printf("Json message is mising fields : %s", string(jsonMessage))
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

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
		data, _ := json.Marshal(kafka.CollectionCompleteMessage{tracker.CollectionId})
		producer.Output <- data
		delete(releases, newCollection.CollectionId)
	} else {
		releases[newCollection.CollectionId] = tracker
	}

}

func trackInprogressRelease(jsonMessage []byte, producer kafka.Producer) {
	var file kafka.FileCompleteMessage
	if err := json.Unmarshal(jsonMessage, &file); err != nil {
		log.Printf("Failed to parse json message")
		return
	}
	if file.CollectionId == "" || file.FileLocation == "" {
		log.Printf("Json message is mising fields : %s", string(jsonMessage))
	}
	mutex.Lock()
	defer mutex.Unlock()
	tracker, exists := releases[file.CollectionId]
	if !exists {
		// Recieved an event for collection that not had it tracker started,
		// so create it as we can start storing the files published.
		tracker = Tracker{file.CollectionId, 0, make([]string, 0)}
	}
	completed := tracker.Update(file)
	if completed {
		data, _ := json.Marshal(kafka.CollectionCompleteMessage{tracker.CollectionId})
		producer.Output <- data
		delete(releases, file.CollectionId)
	} else {
		releases[file.CollectionId] = tracker
	}
}

func main() {
	publishCountTopic := utils.GetEnvironmentVariable("PUBLISH_COUNT_TOPIC", "uk.gov.ons.dp.web.publish-count")
	completeFileTopic := utils.GetEnvironmentVariable("COMPLETE_FILE_TOPIC", "uk.gov.ons.dp.web.complete-file")
	completeCollectionTopic := utils.GetEnvironmentVariable("COMPLETE_TOPIC", "uk.gov.ons.dp.web.complete")
	log.Printf("Starting publish tracker of %q and %q to %q", completeFileTopic, publishCountTopic, completeCollectionTopic)

	fileConsumer := kafka.NewConsumer(completeFileTopic)
	collectionConsumer := kafka.NewConsumer(publishCountTopic)
	//signals := make(chan os.Signal, 1)
	//signal.Notify(signals, os.Interrupt)
	producer := kafka.NewProducer(completeCollectionTopic)

	for {
		select {
		case msg := <-collectionConsumer.Incoming:
			trackNewRelease(msg, producer)
		case msg := <-fileConsumer.Incoming:
			trackInprogressRelease(msg, producer)
			//case <-signals:
			//log.Printf("Stopped publish tracker")
			//return
		}
	}
}
