package kafka

import (
  "encoding/json"
  "log"
  "os"
  "os/signal"
  "github.com/IBM/sarama"
)

type Consumer struct {
  Consumer sarama.Consumer
  Logger *log.Logger
}

func NewConsumer(brokers []string, config *sarama.Config, logger *log.Logger) (*Consumer, error) {
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{Consumer: consumer, Logger: logger}, nil
}

func (c *Consumer) ConsumeMessages(topic string) ([]Message, error) {
	partitionConsumer, err := c.Consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			c.Logger.Println("Error closing partition consumer:", err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var receivedMessages []Message

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var decodedMsg Message
			err := json.Unmarshal(msg.Value, &decodedMsg)
			if err != nil {
				c.Logger.Println("Error decoding message:", err)
				continue
			}
      c.Logger.Println("Consumed Message:", decodedMsg)
			receivedMessages = append(receivedMessages, decodedMsg)
		case <-signals:
			break ConsumerLoop
		}
	}

	return receivedMessages, nil
}
