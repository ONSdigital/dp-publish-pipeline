package kafka

import (
	"log"
	"os"
	"os/signal"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/Shopify/sarama"
)

type ConsumerCallback func([]byte)

func CreateConsumer() sarama.Consumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	brokers := []string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}

	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}
	return master
}

func ProcessMessages(master sarama.Consumer, topic string, messageChannel chan []byte) {
	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	go func() {
		defer consumer.Close()
		log.Printf("Started kafka consumer of topic '%s'", topic)
		for {
			select {
			case err := <-consumer.Errors():
				log.Printf("Error : %s", err.Error())
			case msg := <-consumer.Messages():
				messageChannel <- msg.Value
			case <-signals:
				log.Printf("Closing kafka consumer of topic '%s'", topic)
				return
			}
		}
	}()
}
