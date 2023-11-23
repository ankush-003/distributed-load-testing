package main

import (
	"flag"
	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"log"
	"os"
	"os/signal"
	"github.com/ankush-003/distributed-load-testing/driver"
	"github.com/ankush-003/distributed-load-testing/kafka"
)

var topics = map[string]string{
	"RegisterTopic":   "register-topic",
	"TestConfigTopic": "test-config-topic",
	"TriggerTopic":    "trigger-topic",
	"MetricsTopic":    "metrics-topic",
	"HeartbeatTopic":  "heartbeat",
}

func main() {
	broker := flag.String("broker", "localhost:9092", "kafka broker address")
	flag.Parse()
	brokers := []string{*broker}
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	//config.Producer.Return.Successes = true

	driverNode := driver.DriverNode{
		NodeID: uuid.New().String(),
		NodeIP: "localhost",
	}

	logFile, err := os.OpenFile("Node_"+driverNode.NodeID, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	logger := log.New(logFile, "[DriverNode] ", log.LstdFlags)

	consumer, err := kafka.NewConsumer(brokers, config, logger)
	if err != nil {
		logger.Fatalf("Error creating Kafka consumer: %s", err)
	}
	defer func() {
		if err := consumer.Consumer.Close(); err != nil {
			logger.Fatalf("Error closing Kafka consumer: %s", err)
		}
	}()

	producer, err := kafka.NewProducer(brokers, config, logger)
	if err != nil {
		logger.Fatalf("Error creating Kafka producer: %s", err)
	}
	defer func() {
		if err := producer.Producer.Close(); err != nil {
			logger.Fatalf("Error closing Kafka producer: %s", err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	registerMsg := kafka.RegisterMessage{
		NodeID:      driverNode.NodeID,
		NodeIP:      driverNode.NodeIP,
		MessageType: "DRIVER_NODE_REGISTER",
	}

	enqueued, _ := producer.ProduceRegisterMessages(topics["RegisterTopic"], []kafka.RegisterMessage{registerMsg})
	if enqueued > 0 {
		log.Println("Driver Registered!")
		logger.Printf("Driver node registered with ID: %s\n", driverNode.NodeID)
	}

	metricsStore, err := driver.NewMetricsStore()
	if err != nil {
		logger.Fatalf("Error creating MetricsStore: %s", err)
	}
	defer metricsStore.Db.Close()

	testConfigChan := make(chan kafka.TestConfigMessage)
	triggerReceived := make(chan struct{})

	go driver.WaitForTestConfig(topics["TestConfigTopic"], topics["TriggerTopic"], consumer, testConfigChan, triggerReceived, logger)

ConsumerLoop:
	for {
		select {
		case <-signals:
			break ConsumerLoop
		case <-triggerReceived:
			testConfigMsg, ok := <-testConfigChan
			if !ok {
				log.Println("TestConfig Not Received")
				break ConsumerLoop // Break loop if testConfigChan is closed
			}
			heart := make(chan struct{})

			log.Println("Received Test Config!")
			driver.HandleTestConfig(testConfigMsg, &driverNode, logger)

			go driver.SendHeartbeats(topics["HeartbeatTopic"], &driverNode, producer, heart, logger)

			log.Println("Starting Load Test!")
			driver.HandleTrigger(topics["MetricsTopic"], &driverNode, &testConfigMsg, producer, metricsStore, logger)
		}
	}
	log.Println("Driver Node is Stopping!")
	logger.Println("Driver Node is Stopping!")
}