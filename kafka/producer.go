package kafka

import (
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/Shopify/sarama"
)

type ProducerInterface interface {
	SendMessage([]byte)
	Close() error
}

type Producer struct {
	producer sarama.AsyncProducer
	Topic    string
}

func (p Producer) SendMessage(message []byte) {
	p.producer.Input() <- &sarama.ProducerMessage{Topic: p.Topic, Value: sarama.StringEncoder(message)}
}

func (p Producer) Close() error {
	return p.producer.Close()
}

func CreateProducer(topic string) ProducerInterface {
	producer, err := sarama.NewAsyncProducer([]string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}, nil)
	if err != nil {
		panic(err)
	}
	return Producer{producer, topic}
}
