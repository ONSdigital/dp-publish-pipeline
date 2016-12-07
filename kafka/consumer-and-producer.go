package kafka

import (
	"log"
	"os"
	"os/signal"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/Shopify/sarama"
)

type ProducerCallback func([]byte, sarama.AsyncProducer, string)

func ConsumeAndProduceMessages(master sarama.Consumer, consumeTopic string, produceTopic string, callback ProducerCallback) {
	producer, err := sarama.NewAsyncProducer([]string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}, nil)
	if err != nil {
		panic(err)
	}

	consumer, err := master.ConsumePartition(consumeTopic, 0, sarama.OffsetNewest)
	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	if err != nil {
		log.Printf("consumer")
		panic(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	messageChannel := make(chan struct{})
	go func() {
		log.Printf("Starting kafka consumer")
		for {
			select {
			case err := <-consumer.Errors():
				log.Printf("Error : %s", err.Error())
			case msg := <-consumer.Messages():
				callback(msg.Value, producer, produceTopic)
			case <-signals:
				// The consumer needs to close the producer as setting it up as a defer function
				// triggers a race condition.
				if err := producer.Close(); err != nil {
					log.Fatalln(err)
				}
				log.Printf("Closing kafka consumer")
				messageChannel <- struct{}{}
			}
		}
	}()
	<-messageChannel
}
