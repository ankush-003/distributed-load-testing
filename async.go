package main

import (
	"log"
	"github.com/IBM/sarama"
	"os"
	"os/signal"
	"encoding/json"
	// "time"
)

type message struct {
	Key string `json:"key"`
	Value string `json:"value"`
}

func main() {
	brokers := []string{"localhost:9092"}
	topic := "test"
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(brokers, config)

	if( err != nil) {
		log.Panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Panic(err)
		}
	} ()
	
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, errors int
	
	messages := []message{{Key: "message", Value: "test 1"}, {Key: "message", Value: "test 2"}}

	encodedMessages := make([][]byte, len(messages))
	for i, v := range messages {
		encodedMessage, err := json.Marshal(v)
		if err != nil {
			log.Panic(err)
		}
		encodedMessages[i] = encodedMessage
	}

	log.Printf("===================\nStarting Producer\n===================\n")
	ProducerLoop:
	for i, v := range encodedMessages {
		select {
			case producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(v)}:
				enqueued++
				log.Println("Message Produced:", messages[i])

			case err := <-producer.Errors():
				log.Println("Failed to produce message", err)
				errors++
			case <-signals:
				break ProducerLoop
		}
	}
	
	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}
