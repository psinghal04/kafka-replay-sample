package main

import (
	"fmt"
	"kafka-replay/config"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("Loading config from file...")
	cfg, err := config.GetConfig("config.json")
	if err != nil {
		log.Fatalf("Encountered fatal error initializing configuration, service being terminated: %v", err)
	}

	c := &kafka.ConfigMap{
		"metadata.broker.list": cfg.BrokerHostEndpoint,
		"security.protocol":    "PLAINTEXT",
		"group.id":             cfg.ConsumeGroup,
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"},
	}

	topic := cfg.ConsumeTopic

	p, err := kafka.NewProducer(c)
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}

	fmt.Printf("Created Producer %v\n", p)

	doneChan := make(chan bool)

	go func() {
		defer close(doneChan)
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					log.Printf("Delivered message to topic %s [%d] at offset %v: \"%s\"\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset, m.Value)
				}
				return

			default:
				log.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	value := "Test is a test message. It was produced at " + time.Now().String()
	p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(value)}
	// wait for delivery report goroutine to finish
	log.Println("waiting for resonse on delivery channel...")
	_ = <-doneChan

	p.Close()
}
