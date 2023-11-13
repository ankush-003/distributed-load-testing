package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"encoding/json"
)

type Message struct {
	Title string
	Body string
}

func createKafkaProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func sendMessage(producer sarama.SyncProducer, topic string, message Message) (int32, int64, error) {
    data, err := json.Marshal(message)
    if err != nil {
	    return -1, -1, err
    }

    msg := &sarama.ProducerMessage{
        Topic: topic,
        Value: sarama.StringEncoder(data),
    }
    partition, offset, err := producer.SendMessage(msg)
    if err != nil {
        return -1, -1, err
    }
    return partition, offset, nil
}

func main() {
    producer, err := createKafkaProducer()
    if err != nil {
        panic(err)
    }
    defer producer.Close()
    msg := Message{"testing", "hello world"}	
    partition, offset, err := sendMessage(producer, "test", msg)
    if err != nil {
        panic(err)
    }
    fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}
