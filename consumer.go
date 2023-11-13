package main

import (
	"log"
	"github.com/IBM/sarama"
	"os"
	"os/signal"
	"encoding/json"
)

type message struct {
        Key string `json:"key"`
        Value string `json:"value"`
}


func main() {
	brokers := []string{"localhost:9092"}
	config := sarama.NewConfig()
	topic := "test"

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Panic(err)
	}

	// trap SIGINT to trigger a shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
	ConsumerLoop:
	for {
		select {
			case msg := <-partitionConsumer.Messages():
				var decodedMsg message
				err := json.Unmarshal(msg.Value, &decodedMsg)
				if err != nil {
					log.Panic(err)
				}

				log.Println("Consumed message:", decodedMsg)
				consumed++
			case <-signals:
				break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)
}
