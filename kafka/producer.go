package kafka

import (
        "log"
        "github.com/IBM/sarama"
        "os"
        "os/signal"
        "encoding/json"
        // "time"
)

type Producer struct {
	Producer sarama.AsyncProducer // Producer instance
	Logger *log.Logger // logger instance
}

func NewProducer(brokers []string, config *sarama.Config, logger *log.Logger) (*Producer, error) {
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	p := &Producer{Producer: producer, Logger: logger}

	return p, nil
}

func (p *Producer) Close() error {
	if err := p.Producer.Close(); err != nil {
		p.Logger.Fatalf("Error closing producer: %v\n", err)
		return err
	}
	p.Logger.Println("Producer closed successfully")
	return nil
}

func (p *Producer) ProduceMessages(topic string, messages []Message) (int, int) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

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
