package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"github.com/IBM/sarama"
)

func main() {
	broker := flag.String("broker", "localhost:9092", "broker address")
	topic := flag.String("topic", "test", "topic name")
	numPartitions := flag.Int("partitions", 1, "number of partitions")

	flag.Parse()

	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin([]string{*broker}, config)
	if err != nil {
		log.Fatalf("Error creating cluster admin: %v", err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Printf("Error closing cluster admin: %v", err)
		}
	}()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(*numPartitions),
		ReplicationFactor: 1, // Adjust the replication factor as needed
	}

	err = admin.CreateTopic(*topic, topicDetail, false)
	if err != nil {
		log.Fatalf("Error creating topic: %v", err)
	}

	log.Printf("Topic '%s' created with %d partitions", *topic, *numPartitions)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals // Wait for a signal to exit
}