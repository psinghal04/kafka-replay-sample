
# Overview #
This is a sample implementation of a Kafka replay consumer in Golang. It consists of two programs:
* A consumer that can be configured to run in the replay mode.
* A producer that produces a simple message to the configured Kafka topic every time it is run.

The consumer implementation demonstrates how a Kafka consumer program can be designed to reset committed offsets in its assigned Kafka topic partitions to a timestamp-based
offset (or the earliest offset, if needed), thus causing past messages to be replayed (or re-consumed).

# Prerequisites #
* Kafka must be deployed and running. The sample was tested with v2.3.0.
* Golang. The sample was tested with v1.12.1
* librdkafka native library. See https://github.com/confluentinc/confluent-kafka-go for details. The sample was tested with librdkafka v1.1.0
* The test Kafka topic with appropriate number of partitions must be created. Refer to Kafka documention for steps for creating partitioned topics. Note: The sample uses PLAINTEXT security
protocol for simplicity.

# Configuration #
The consumer can be configured via the config.json file. The "replay-type" supports two options: "timestamp", and "beginning". The other
replay-related properties become relevant only when replay-mode is "true". When "replay-mode" is true and "replay-type" is timestamp, the consumer will reset the committed offset
to the ealiest message that has a timestamp that is equal to or later than the "replay-from" timestamp.
```
{
  "broker-host-endpoint": "localhost:9092",
  "consumer-group": "replay-group",
  "topic": "casenetNextGen",
  "replay-mode": true,
  "replay-type": "timestamp",
  "replay-from": "2019-07-17T14:59:05Z"
}
```

# Testing the Sample Implementation #
1. Ensure the pre-requisites are fulfilled.
2. Execute "go get -d" to pull Go dependencies.
3. Execute "go run producer.go" to produce a message to the topic. You can run this multiple times spaced over some time to produce messages spread over a period of time.
4. Adjust the "replay-from" timestamp to your test conditions.
5. Execute "go run consumer.go" to launch the consumer. You can open as many command shells as the number of consumers you wish to run, executing this command in each 
shell to launch an instance of the consumer process to join the consumer group (note that once there are as many consumers as the number of partitions, any additional consumers that are added to the group may result in one or more consumers remaining idle with no assigned partitions). Observe how partitions are re-assigned every time a new consumer is launched, and how messages are replayed, some times in duplicate, as a result of partition re-assignment. Adjust the
"replay-from" timestamp to tune the replayed messages and observe the effects by re-launching consumers.

