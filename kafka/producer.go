package kafka

import (
	"log"
	"strconv"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/Shopify/sarama"
)

type Producer struct {
	producer sarama.AsyncProducer
	Output   chan []byte
	Closer   chan bool
}

func NewProducer(topic string) Producer {
	envMax, err := strconv.ParseInt(utils.GetEnvironmentVariable("KAFKA_MAX_BYTES", "2000000"), 10, 32)
	if err != nil {
		panic("Bad value for KAFKA_MAX_BYTES")
	}
	config := sarama.NewConfig()
	config.Producer.MaxMessageBytes = int(envMax)
	producer, err := sarama.NewAsyncProducer([]string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}, config)
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
			case err := <-producer.Errors():
				log.Panicf("Producer[outer] %q: %s", topic, err)
			case message := <-outputChannel:

				select {
				case err := <-producer.Errors():
					log.Panicf("Producer[inner] %q: %s", topic, err)
				case producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(message)}:
				}
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
