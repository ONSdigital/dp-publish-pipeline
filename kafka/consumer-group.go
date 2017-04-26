package kafka

import (
	"fmt"
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
	Errors   chan error
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

func NewConsumerGroup(topic string, group string) (*ConsumerGroup, error) {
	config := cluster.NewConfig()
	config.Group.Return.Notifications = true
	config.Consumer.Return.Errors = true
	config.Consumer.MaxWaitTime = 50 * time.Millisecond
	brokers := []string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}

	consumer, err := cluster.NewConsumer(brokers, group, []string{topic}, config)
	if err != nil {
		return nil, fmt.Errorf("Bad NewConsumer of %q: %s", topic, err)
	}

	cg := ConsumerGroup{
		Consumer: consumer,
		Incoming: make(chan Message),
		Closer:   make(chan bool),
		Errors:   make(chan error),
	}
	signals := make(chan os.Signal, 1)
	//signal.Notify(signals, os.)

	go func() {
		defer cg.Consumer.Close()
		log.Printf("Started kafka consumer of topic %q group %q", topic, group)
		for {
			select {
			case err := <-cg.Consumer.Errors():
				log.Printf("Error: %s", err.Error())
				cg.Errors <- err
			default:
				select {
				case msg := <-cg.Consumer.Messages():
					cg.Incoming <- Message{msg, cg.Consumer}
				case n, more := <-cg.Consumer.Notifications():
					if more {
						log.Printf("Rebalancing %q group %q - partitions %+v", topic, group, n.Current[topic])
					}
				case <-time.After(tick):
					cg.Consumer.CommitOffsets()
				case <-signals:
					log.Printf("Quitting kafka consumer of topic %q group %q", topic, group)
					return
				case <-cg.Closer:
					log.Printf("Closing kafka consumer of topic %q group %q", topic, group)
					return
				}
			}
		}
	}()
	return &cg, nil
}
