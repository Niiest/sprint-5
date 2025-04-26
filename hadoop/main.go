package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

func main() {
	// Kafka consumer configuration
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9093",
		"group.id":           "hadoop-consumer-group-3",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
		"session.timeout.ms": 6000,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}

	// Subscribe to the topic
	err = consumer.Subscribe("hadoop-topic", nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
	}

	// Initialize HDFS client with custom dial function
	dialFunc := (&net.Dialer{
		Timeout:   60 * time.Second,
		KeepAlive: 60 * time.Second,
	}).DialContext

	clientOptions := hdfs.ClientOptions{
		Addresses:           []string{"localhost:9000"},
		User:                "root",
		NamenodeDialFunc:    dialFunc,
		DatanodeDialFunc:    dialFunc,
		UseDatanodeHostname: true,
	}

	hdfsClient, err := hdfs.NewClient(clientOptions)
	if err != nil {
		log.Fatalf("Failed to create HDFS client: %s", err)
	}

	defer consumer.Close()
	dataDir := "/data"
	if err := hdfsClient.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("Failed to create directory %s: %s", dataDir, err)
	}

	// Poll for messages
	for {
		msg, err := consumer.ReadMessage(100 * time.Millisecond)
		if err == nil {
			value := string(msg.Value)
			fmt.Printf("Received message: value=%s, partition=%v\n", value, msg.TopicPartition)

			// Retry logic for HDFS write
			var writer *hdfs.FileWriter

			hdfsFile := fmt.Sprintf("/data/message_%s", uuid.New().String())
			writer, err = hdfsClient.Create(hdfsFile)

			if err != nil {
				log.Printf("Failed to create HDFS file: %s", err)
				continue
			}

			_, err = writer.Write([]byte(value + "\n"))
			if err != nil {
				log.Printf("Failed to write to HDFS file: %s", err)
				writer.Close()
				continue
			}

			err = writer.Close()
			if err != nil {
				log.Printf("Failed to close HDFS file: %s", err)
			}

			fmt.Printf("Message '%s' written to HDFS at path: '%s' \n", value, hdfsFile)
		} else {
			// Handle errors
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.IsFatal() {
				log.Fatalf("Fatal error: %s", kafkaErr)
			}
		}
	}
}
