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

//creating consumers to consume RegiterMessages, TestConfigMessages, TriggerMessages, MetricsMessages, and HeartbeatMessages

func (c *Consumer) ConsumeRegisterMessages(topic string, messageChan chan<- RegisterMessage, doneChan chan struct{}, numberOfDrivers int) {

	partitionConsumer, err := c.Consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		c.Logger.Println("Error creating partition consumer:", err)
		close(doneChan)
		return
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			c.Logger.Println("Error closing partition consumer:", err)
		}
		close(doneChan)
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var consumedMessages int

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var decodedMsg RegisterMessage
			err := json.Unmarshal(msg.Value, &decodedMsg)
			if err != nil {
				c.Logger.Println("Error decoding message:", err)
				continue
			}
			c.Logger.Println("Consumed Message:", decodedMsg)
			messageChan <- decodedMsg // Sending the decoded message to the channel

			// Increment the count of consumed messages
			consumedMessages++

			// Check if the desired number of messages has been consumed
			if consumedMessages >= numberOfDrivers {
				break ConsumerLoop
			}
		case <-signals:
			break ConsumerLoop
		}
	}
}


func (c *Consumer) ConsumeTestConfigMessages(topic string, messageChan chan<- TestConfigMessage, doneChan chan struct{}) {
	partitionConsumer, err := c.Consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		c.Logger.Println("Error creating partition consumer:", err)
		close(doneChan)
		return
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			c.Logger.Println("Error closing partition consumer:", err)
		}
		close(doneChan)
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

ConsumerLoop:

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var decodedMsg TestConfigMessage
			err := json.Unmarshal(msg.Value, &decodedMsg)
			if err != nil {
				c.Logger.Println("Error decoding message:", err)
				continue
			}
			c.Logger.Println("Consumed Message:", decodedMsg)
			messageChan <- decodedMsg // Sending the decoded message to the channel
		case <-signals:
			break ConsumerLoop
		}
	}
}

func (c *Consumer) ConsumeTriggerMessages(topic string, messageChan chan<- TriggerMessage, doneChan chan struct{}) {
	partitionConsumer, err := c.Consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		c.Logger.Println("Error creating partition consumer:", err)
		close(doneChan)
		return
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			c.Logger.Println("Error closing partition consumer:", err)
		}
		close(doneChan)
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

ConsumerLoop:

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var decodedMsg TriggerMessage
			err := json.Unmarshal(msg.Value, &decodedMsg)
			if err != nil {
				c.Logger.Println("Error decoding message:", err)
				continue
			}
			c.Logger.Println("Consumed Message:", decodedMsg)
			messageChan <- decodedMsg // Sending the decoded message to the channel
		case <-signals:
			break ConsumerLoop
		}
	}
}

func (c *Consumer) ConsumeTestConfigAndTriggerMessages(testConfigTopic string, triggerTopic string, testConfigChan chan<- TestConfigMessage, triggerReceived chan<- struct{}) {
	partitionConsumerTestConfig, err := c.Consumer.ConsumePartition(testConfigTopic, 0, sarama.OffsetNewest)
	if err != nil {
		c.Logger.Println("Error creating partition consumer for test config:", err)
		return
	}
	defer func() {
		if err := partitionConsumerTestConfig.Close(); err != nil {
			c.Logger.Println("Error closing partition consumer for test config:", err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumerTestConfig.Messages():
			var decodedTestConfig TestConfigMessage
			err := json.Unmarshal(msg.Value, &decodedTestConfig)
			if err != nil {
				c.Logger.Println("Error decoding test config message:", err)
				continue
			}
			c.Logger.Println("Consumed Test Config Message:", decodedTestConfig)
			testConfigChan <- decodedTestConfig // Sending the decoded test config message to the channel

			// After receiving a test config message, start consuming from the trigger topic
			partitionConsumerTrigger, err := c.Consumer.ConsumePartition(triggerTopic, 0, sarama.OffsetNewest)
			if err != nil {
				c.Logger.Println("Error creating partition consumer for trigger:", err)
				return
			}
			defer func() {
				if err := partitionConsumerTrigger.Close(); err != nil {
					c.Logger.Println("Error closing partition consumer for trigger:", err)
				}
			}()

			for {
				select {
				case msg := <-partitionConsumerTrigger.Messages():
					var decodedTrigger TriggerMessage
					err := json.Unmarshal(msg.Value, &decodedTrigger)
					if err != nil {
						c.Logger.Println("Error decoding trigger message:", err)
						continue
					}
					c.Logger.Println("Consumed Trigger Message:", decodedTrigger)

					// Compare Test IDs and start load testing if they match
					if decodedTrigger.TestID == decodedTestConfig.TestID {
						triggerReceived <- struct{}{} // Send a trigger received signal
						break ConsumerLoop           // Exit the loop upon receiving the trigger
					}
				case <-signals:
					return // Stop consuming messages on interrupt signal
				}
			}
		case <-signals:
			break ConsumerLoop // Stop consuming messages on interrupt signal
		}
	}
}

func (c *Consumer) ConsumeMetricsMessages(topic string, messageChan chan<- MetricsMessage, doneChan chan struct{}) {
	partitionConsumer, err := c.Consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		c.Logger.Println("Error creating partition consumer:", err)
		close(doneChan)
		return
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			c.Logger.Println("Error closing partition consumer:", err)
		}
		close(doneChan)
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

ConsumerLoop:


	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var decodedMsg MetricsMessage
			err := json.Unmarshal(msg.Value, &decodedMsg)
			if err != nil {
				c.Logger.Println("Error decoding message:", err)
				continue
			}
			c.Logger.Println("Consumed Message:", decodedMsg)
			messageChan <- decodedMsg // Sending the decoded message to the channel
		case <-signals:
			break ConsumerLoop
		}
	}
}

func (c *Consumer) ConsumeHeartbeatMessages(topic string, messageChan chan<- HeartbeatMessage, doneChan chan struct{}) {
	partitionConsumer, err := c.Consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		c.Logger.Println("Error creating partition consumer:", err)
		close(doneChan)
		return
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			c.Logger.Println("Error closing partition consumer:", err)
		}
		close(doneChan)
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

ConsumerLoop:


	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var decodedMsg HeartbeatMessage
			err := json.Unmarshal(msg.Value, &decodedMsg)
			if err != nil {
				c.Logger.Println("Error decoding message:", err)
				continue
			}
			c.Logger.Println("Consumed Message:", decodedMsg)
			messageChan <- decodedMsg // Sending the decoded message to the channel
		case <-signals:
			break ConsumerLoop
		}
	}
}

