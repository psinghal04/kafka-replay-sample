package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"log"
	"strconv"
	"time"

	"kafka-replay-sample/config"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	replayFromTimestamp = "timestamp"
	replayFromBeginning = "beginning"
)

func main() {
	fmt.Println("Loading config from file...")
	cfg, err := config.GetConfig("config.json")
	if err != nil {
		log.Fatalf("Encountered fatal error initializing configuration, service being terminated: %v", err)
	}

	config := &kafka.ConfigMap{
		"metadata.broker.list":            cfg.BrokerHostEndpoint,
		"security.protocol":               "PLAINTEXT",
		"group.id":                        cfg.ConsumeTopic,
		"auto.offset.reset":               "earliest",
		"go.application.rebalance.enable": true,
		"go.events.channel.enable":        true,
	}
	topic := cfg.ConsumeTopic

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s\n", err)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("Failed to subcribe consumer to topic %s due to error: %v\n", topic, err)
	}

	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating\n", sig)
			run = false
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				partitionsToAssign := e.Partitions
				if len(partitionsToAssign) == 0 {
					log.Printf("No partitions assigned\n")
					continue
				}

				log.Printf("Assigned/Re-assigned Partitions: %s\n", getPartitionNumbers(partitionsToAssign))
				//if the consumer was launched in replay mode, it needs to figure out which offset to replay from in each assigned partition, and then
				//reset the offset to that point for each partition.
				if cfg.ReplayMode {
					switch cfg.ReplayType {
					case replayFromBeginning:
						log.Println("Replay from beginning, resetting offsets to beginning")
						//reset offsets of all assigned partitions to "beginning"
						partitionsToAssign, err = resetPartitionOffsetsToBeginning(c, e.Partitions)
						if err != nil {
							log.Fatalf("error trying to reset offsets to beginning: %v\n", err)
						}
					case replayFromTimestamp:
						log.Printf("Replay from timestamp %s, resetting offsets to that point\n", cfg.ReplayFrom)
						t, err := time.Parse(time.RFC3339Nano, cfg.ReplayFrom)
						if err != nil {
							log.Fatalf("failed to parse replay timestamp %s due to error %v", cfg.ReplayFrom, err)
						}
						//reset offsets of all assigned partitions to the specified timestamp in the past
						partitionsToAssign, err = resetPartitionOffsetsToTimestamp(c, e.Partitions, t.UnixNano()/int64(time.Millisecond))
						if err != nil {
							log.Fatalf("error trying to reset offsets to timestamp: %v\n", err)
						}
					}
				}

				c.Assign(partitionsToAssign)
			case kafka.RevokedPartitions:
				c.Unassign()
			case *kafka.Message:
				fmt.Printf("%% Message on %s: %s\n", e.TopicPartition, string(e.Value))
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			}
		}
	}
	fmt.Printf("Closing consumer\n")
	c.Close()
}

func resetPartitionOffsetsToTimestamp(c *kafka.Consumer, partitions []kafka.TopicPartition, timestamp int64) ([]kafka.TopicPartition, error) {
	var prs []kafka.TopicPartition
	for _, par := range partitions {
		prs = append(prs, kafka.TopicPartition{Topic: par.Topic, Partition: par.Partition, Offset: kafka.Offset(timestamp)})
	}

	updtPars, err := c.OffsetsForTimes(prs, 5000)
	if err != nil {
		log.Printf("Failed to reset offsets to supplied timestamp due to error: %v\n", err)
		return partitions, err
	}

	return updtPars, nil
}

func resetPartitionOffsetsToBeginning(c *kafka.Consumer, partitions []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	var prs []kafka.TopicPartition
	for _, par := range partitions {
		prs = append(prs, kafka.TopicPartition{Topic: par.Topic, Partition: par.Partition, Offset: kafka.OffsetBeginning})
	}

	return prs, nil
}

func getPartitionNumbers(pars []kafka.TopicPartition) string {
	var pNums string
	for i, par := range pars {
		if i == len(pars)-1 {
			pNums = pNums + strconv.Itoa(int(par.Partition))
		} else {
			pNums = pNums + strconv.Itoa(int(par.Partition)) + ", "
		}
	}

	return pNums
}
