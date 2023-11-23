package main

import (
	"flag"
	"log"
	"os"
	"github.com/ankush-003/distributed-load-testing/kafka"

	"github.com/IBM/sarama"
)

func main() {
	topic := flag.String("topic", "test", "Consumer topic name")
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

	messageChan := make(chan kafka.MetricsMessage)
	doneChan := make(chan struct{})

	go consumer.ConsumeMetricsMessages(*topic, messageChan, doneChan)

ConsumerLoop:
	for {
		select {
		case msg := <-messageChan:
			logger.Println("Received Register Message:", msg)
		case <-doneChan:
			break ConsumerLoop
		}
	}
}
