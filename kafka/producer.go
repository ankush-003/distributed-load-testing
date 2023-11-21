package kafka

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"github.com/IBM/sarama"
)

type Producer struct {
	Producer sarama.AsyncProducer // Producer instance
	Logger   *log.Logger          // logger instance
}

func NewProducer(brokers []string, config *sarama.Config, logger *log.Logger) (*Producer, error) {
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	p := &Producer{Producer: producer, Logger: logger}

	return p, nil
}



func (p *Producer) ProduceRegisterMessages(topic string, messages []RegisterMessage) (int, int) {
	signals := make(chan os.Signal, 1) //signal channel
	signal.Notify(signals, os.Interrupt) //notify signal

	var enqueued, errors int

	encodedMessages := make([][]byte, len(messages))

	for i, v := range messages {
		encodedMessage, err := json.Marshal(v)
		if err != nil {
			p.Logger.Printf("Error marshalling message %d: %s\n", i, err)
			continue
		}
		encodedMessages[i] = encodedMessage		
	}

ProducerLoop:
	for i, v := range encodedMessages {
		select {
		case p.Producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(v)}:
			enqueued++
			// p.Logger.Printf("Message enqueued: %v\n",messages[i])
		case err := <-p.Producer.Errors():
			p.Logger.Printf("Failed to produce message %v: %s\n", messages[i], err)
			errors++
		case <-signals:
			break ProducerLoop
		}
	}

	return enqueued, errors
}


func (p *Producer) ProduceTestConfigMessages(topic string, messages []TestConfigMessage) (int, int) {
	signals := make(chan os.Signal, 1) //signal channel
	signal.Notify(signals, os.Interrupt) //notify signal

	var enqueued, errors int

	encodedMessages := make([][]byte, len(messages))

	for i, v := range messages {
		encodedMessage, err := json.Marshal(v)
		if err != nil {
			p.Logger.Printf("Error marshalling message %d: %s\n", i, err)
			continue
		}
		encodedMessages[i] = encodedMessage		
	}

ProducerLoop:
	for i, v := range encodedMessages {	
		select {
		case p.Producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(v)}:
			enqueued++
			// p.Logger.Printf("Message enqueued: %v\n",messages[i])
		case err := <-p.Producer.Errors():
			p.Logger.Printf("Failed to produce message %v: %s\n", messages[i], err)
			errors++
		case <-signals:
			break ProducerLoop
		}
	}

	return enqueued, errors
}

func (p *Producer) ProduceTriggerMessages(topic string, messages []TriggerMessage) (int, int) {
	signals := make(chan os.Signal, 1) //signal channel
	signal.Notify(signals, os.Interrupt) //notify signal

	var enqueued, errors int

	encodedMessages := make([][]byte, len(messages))

	for i, v := range messages {
		encodedMessage, err := json.Marshal(v)
		if err != nil {
			p.Logger.Printf("Error marshalling message %d: %s\n", i, err)
			continue
		}
		encodedMessages[i] = encodedMessage		
	}

ProducerLoop:
	for i, v := range encodedMessages {
		select {
		case p.Producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(v)}:
			enqueued++
			// p.Logger.Printf("Message enqueued: %v\n",messages[i])
		case err := <-p.Producer.Errors():
			p.Logger.Printf("Failed to produce message %v: %s\n", messages[i], err)
			errors++
		case <-signals:
			break ProducerLoop
		}
	}

	return enqueued, errors
}

func (p *Producer) ProduceMetricsMessages(topic string, messages []MetricsMessage) (int, int) {

	signals := make(chan os.Signal, 1) //signal channel
	signal.Notify(signals, os.Interrupt) //notify signal

	var enqueued, errors int

	encodedMessages := make([][]byte, len(messages))

	for i, v := range messages {
		encodedMessage, err := json.Marshal(v)
		if err != nil {
			p.Logger.Printf("Error marshalling message %d: %s\n", i, err)
			continue
		}
		encodedMessages[i] = encodedMessage		
	}

ProducerLoop:
	for i, v := range encodedMessages {
		select {
		case p.Producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(v)}:
			enqueued++
			// p.Logger.Printf("Message enqueued: %v\n",messages[i])
		case err := <-p.Producer.Errors():
			p.Logger.Printf("Failed to produce message %v: %s\n", messages[i], err)
			errors++
		case <-signals:
			break ProducerLoop
		}
	}

	return enqueued, errors
}

func (p *Producer) ProduceHeartbeatMessages(topic string, messages []HeartbeatMessage) (int, int) {
	
	signals := make(chan os.Signal, 1) //signal channel
	signal.Notify(signals, os.Interrupt) //notify signal

	var enqueued, errors int

	encodedMessages := make([][]byte, len(messages))

	for i, v := range messages {
		encodedMessage, err := json.Marshal(v)
		if err != nil {
			p.Logger.Printf("Error marshalling message %d: %s\n", i, err)
			continue
		}
		encodedMessages[i] = encodedMessage		
	}

// ProducerLoop:
	for i, v := range encodedMessages {	
		select {
		case p.Producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(v)}:
			enqueued++
			p.Logger.Printf("Message enqueued: %v\n",messages[i])
		case err := <-p.Producer.Errors():
			p.Logger.Printf("Failed to produce message %v: %s\n", messages[i], err)
			errors++
		default:

		}
	}

	return enqueued, errors
}





func (p *Producer) ProduceHeartbeatMessagesPartitions(topic string, messages []HeartbeatMessage) (int, int) {
	signals := make(chan os.Signal, 1) // Signal channel
	signal.Notify(signals, os.Interrupt) // Notify signal

	var enqueued, errors int
	encodedMessages := make([][]byte, len(messages))

	for i, v := range messages {
		encodedMessage, err := json.Marshal(v)
		if err != nil {
			p.Logger.Printf("Error marshalling message %d: %s\n", i, err)
			continue
		}
		encodedMessages[i] = encodedMessage
	}

ProducerLoop:
	for i, v := range encodedMessages {
		select {
		case p.Producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(messages[i].NodeID), // Set driver node's ID as the key
			Value: sarama.StringEncoder(v),
		}:
			enqueued++
		case err := <-p.Producer.Errors():
			p.Logger.Printf("Failed to produce message %v: %s\n", messages[i], err)
			errors++
		case <-signals:
			break ProducerLoop
		}
	}

	return enqueued, errors
}