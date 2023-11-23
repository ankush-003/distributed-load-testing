package main

import (
	"flag"
	"log"
	"os"
	"github.com/IBM/sarama"
	"github.com/ankush-003/distributed-load-testing/kafka"
)

func main() {
	topic := flag.String("topic", "test", "Producer topic name")
	broker := flag.String("broker", "localhost:9092", "Kafka broker address")
	flag.Parse()

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	logger := log.New(os.Stdout, "KafkaProducer: ", log.Ldate|log.Ltime|log.Lshortfile)

	producer, err := kafka.NewProducer([]string{*broker}, config, logger)

	if err != nil {
		logger.Fatalf("Error creating producer: %s", err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			logger.Fatalf("Error closing producer: %s", err)
		}
	}()

	messages := []kafka.MetricsMessage{
		{
			NodeID:   "node1",
			TestID:   "test1",
			ReportID: "report1",
			Metrics: kafka.MetricsData{
				MeanLatency:   "1",
				MedianLatency: "2",
				MinLatency:    "3",
				MaxLatency:    "4",
			},

		},
		{
			NodeID:   "node2",
			TestID:   "test2",
			ReportID: "report2",
			Metrics: kafka.MetricsData{
				MeanLatency:   "5",
				MedianLatency: "6",
				MinLatency:    "7",
				MaxLatency:    "8",
			},
		},
	}

	enqueued, errors := producer.ProduceMetricsMessages(*topic, messages)

	logger.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}
