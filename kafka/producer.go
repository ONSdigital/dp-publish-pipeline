package kafka

import (
	"log"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/Shopify/sarama"
)

type Producer struct {
	producer sarama.AsyncProducer
	Output   chan []byte
	Closer   chan bool
}

func NewProducer(topic string) Producer {
	producer, err := sarama.NewAsyncProducer([]string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}, nil)
	if err != nil {
		panic(err)
	}
	outputChannel := make(chan []byte)
	closerChannel := make(chan bool)
	//signals := make(chan os.Signal, 1)
	//signal.Notify(signals, os.Interrupt)
	go func() {
		defer producer.Close()
		log.Printf("Started kafka producer of topic %q", topic)
		for {
			select {
			case message := <-outputChannel:
				producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(message)}
			case <-closerChannel:
				log.Printf("Closing kafka producer of topic %q", topic)
				return
				//case <-signals:
				//log.Printf("Quitting kafka producer of topic %q", topic)
				//return
			}
		}
	}()
	return Producer{producer, outputChannel, closerChannel}
}
