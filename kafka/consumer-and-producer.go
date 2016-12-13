package kafka

import (
	"log"
	"os"
	"os/signal"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/Shopify/sarama"
)

type ProducerCallback func([]byte, sarama.AsyncProducer, string)

type ConsumeMessage struct {
	Payload  []byte
	Producer sarama.AsyncProducer
}

func ConsumeAndProduceMessages(master sarama.Consumer, consumeTopic, produceTopic string, callback ProducerCallback) {
	ConsumeMessagesMulti(master, consumeTopic, produceTopic, callback, nil)
}

func ConsumeAndChannelMessages(master sarama.Consumer, consumeTopic, produceTopic string, ch chan ConsumeMessage) {
	ConsumeMessagesMulti(master, consumeTopic, produceTopic, nil, ch)
}

func ConsumeMessagesMulti(master sarama.Consumer, consumeTopic, produceTopic string, callback ProducerCallback, ch chan ConsumeMessage) {
	producer, err := sarama.NewAsyncProducer([]string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}, nil)
	if err != nil {
		panic(err)
	}

	consumer, err := master.ConsumePartition(consumeTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("consumer")
		panic(err)
	}
	defer func() {
		if consumerErr := consumer.Close(); err != nil {
			panic(consumerErr)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	messageChannel := make(chan bool)
	go func() {
		log.Printf("Starting kafka consumer - topics '%s' to '%s'", consumeTopic, produceTopic)
		for {
			select {
			case err := <-consumer.Errors():
				log.Printf("Error : %s", err.Error())
			case msg := <-consumer.Messages():
				if callback != nil {
					callback(msg.Value, producer, produceTopic)
				} else {
					ch <- ConsumeMessage{Payload: msg.Value, Producer: producer}
				}
			case <-signals:
				// The consumer needs to close the producer as setting it up as a defer function
				// triggers a race condition.
				if callback != nil {
					if err := producer.Close(); err != nil {
						log.Fatalln(err)
					}
				} else {
					ch <- ConsumeMessage{Payload: nil, Producer: nil}
				}
				log.Printf("Closing kafka consumer")
				messageChannel <- true
			}
		}
	}()
	<-messageChannel
}
