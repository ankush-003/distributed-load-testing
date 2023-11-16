package main

import (
  "tester/kafka"
  "github.com/IBM/sarama"
  "log"
  "os"
  "flag"
)

func main() {
	topic := flag.String("topic", "test", "Producer topic name")
	broker := flag.String("broker", "localhost:9092", "Kafka broker address")
	flag.Parse()
  
  brokers := []string{*broker}
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	logger := log.New(os.Stdout, "KafkaConsumer: ", log.Ldate|log.Ltime|log.Lshortfile)

	consumer, err := kafka.NewConsumer(brokers, config, logger)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := consumer.Consumer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	receivedMessages, err := consumer.ConsumeMessages(*topic)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Received Messages:", receivedMessages)
}
