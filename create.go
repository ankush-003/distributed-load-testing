package main

import (
    "fmt"
    //"time"

    "github.com/IBM/sarama"
)

func main() {
    brokerAddrs := []string{"localhost:9092"}
    config := sarama.NewConfig()

    admin, err := sarama.NewClusterAdmin(brokerAddrs, config)
    if err != nil {
        panic(err)
    }
    defer func() { _ = admin.Close() }()

    topicName := "test"
    topicDetail := &sarama.TopicDetail{
        NumPartitions:     1,
        ReplicationFactor: 1,
    }

    err = admin.CreateTopic(topicName, topicDetail, false)
    if err != nil {
        panic(err)
    }

    fmt.Printf("Topic %s created successfully\n", topicName)
}

