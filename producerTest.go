package main

import (
  "log"
  "flag"
  "os"
  "github.com/IBM/sarama"
  "tester/kafka"
)

func main() {
  topic := flag.String("topic", "test", "Producer topic name")
  broker := flag.String("broker", "localhost:9092", "Kafka broker address")
  flag.Parse()
  
  config := sarama.NewConfig()
  config.Producer.Return.Successes = true

  logger := log.New(os.Stdout, "KafkaProducer: ", log.Ldate | log.Ltime | log.Lshortfile)

  producer, err := kafka.NewProducer([]string{*broker}, config, logger)

	if err != nil {
		logger.Fatalf("Error creating producer: %s", err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			logger.Fatalf("Error closing producer: %s", err)
		}
	}()

	messages := []kafka.Message{
		{Key: "message", Value: "test 1"},
		{Key: "message", Value: "test 2"},
	}

	enqueued, errors := producer.ProduceMessages(*topic, messages)

	logger.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}
