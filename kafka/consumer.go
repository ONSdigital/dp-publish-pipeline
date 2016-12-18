package kafka

import (
	"log"
	"os"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/Shopify/sarama"
)

type Consumer struct {
	Master   sarama.Consumer
	Consumer sarama.PartitionConsumer
	Incoming chan []byte
	Closer   chan bool
}

func NewConsumer(topic string) Consumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	brokers := []string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	messageChannel := make(chan []byte)
	closerChannel := make(chan bool)
	signals := make(chan os.Signal, 1)
	//signal.Notify(signals, os.Interrupt)

	go func() {
		defer consumer.Close()
		log.Printf("Started kafka consumer of topic %q", topic)
		for {
			select {
			case err := <-consumer.Errors():
				log.Printf("Error: %s", err.Error())
				return
			default:
				select {
				case msg := <-consumer.Messages():
					messageChannel <- msg.Value
				case <-signals:
					log.Printf("Quitting kafka consumer of topic %q", topic)
					return
				case <-closerChannel:
					log.Printf("Closing kafka consumer of topic %q", topic)
					return
				}
			}
		}
	}()
	return Consumer{Master: master, Consumer: consumer, Incoming: messageChannel, Closer: closerChannel}
}
