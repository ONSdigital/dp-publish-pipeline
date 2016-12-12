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
	ConsumeAndProduceMessagesMulti(master, consumeTopic, produceTopic, callback, nil)
}

func ConsumeAndProduceMessagesByChannel(master sarama.Consumer, consumeTopic string, ch chan []byte) {
	ConsumeAndProduceMessagesMulti(master, consumeTopic, "", nil, ch)
}

func ConsumeAndProduceMessagesMulti(master sarama.Consumer, consumeTopic string, produceTopic string, callback ProducerCallback, ch chan []byte) {
	var producer sarama.AsyncProducer
	var err error
	if callback != nil {
		producer, err = sarama.NewAsyncProducer([]string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}, nil)
		if err != nil {
			panic(err)
		}
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
					ch <- msg.Value
				}
			case <-signals:
				// The consumer needs to close the producer as setting it up as a defer function
				// triggers a race condition.
				if callback != nil {
					if err := producer.Close(); err != nil {
						log.Fatalln(err)
					}
				} else {
					ch <- nil
				}
				log.Printf("Closing kafka consumer")
				messageChannel <- true
			}
		}
	}()
	<-messageChannel
}
