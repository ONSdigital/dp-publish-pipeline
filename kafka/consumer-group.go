package kafka

import (
	"log"
	"os"
	"time"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

var tick = time.Millisecond * 4000

type ConsumerGroup struct {
	Consumer *cluster.Consumer
	Incoming chan Message
	Closer   chan bool
}

type Message struct {
	message  *sarama.ConsumerMessage
	consumer *cluster.Consumer
}

func (M Message) GetData() []byte {
	return M.message.Value
}

func (M Message) Commit() {
	M.consumer.MarkOffset(M.message, "metadata")
	//M.consumer.CommitOffsets()
	//log.Printf("Offset : %d, Partition : %d", M.message.Offset, M.message.Partition)
}

func NewConsumerGroup(topic string, group string) ConsumerGroup {
	config := cluster.NewConfig()
	config.Group.Return.Notifications = true
	config.Consumer.Return.Errors = true
	config.Consumer.MaxWaitTime = 50 * time.Millisecond
	brokers := []string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}
	consumer, err := cluster.NewConsumer(brokers, group, []string{topic}, config)

	if err != nil {
		panic(err)
	}

	messageChannel := make(chan Message)
	closerChannel := make(chan bool)
	signals := make(chan os.Signal, 1)
	//signal.Notify(signals, os.)

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
					messageChannel <- Message{msg, consumer}
				case n := <-consumer.Notifications():
					log.Printf("Rebalancing - partitions : %+v", n.Current[topic])
				case <-time.After(tick):
					consumer.CommitOffsets()
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
	return ConsumerGroup{Consumer: consumer, Incoming: messageChannel, Closer: closerChannel}
}
