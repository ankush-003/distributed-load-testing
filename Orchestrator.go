package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"tester/kafka"
	"time"
	"github.com/IBM/sarama"
	"github.com/dgraph-io/badger/v3"
	"github.com/gin-gonic/gin"
	"tester/orchestrator"
)


func main() {

	// Creating BadgerDB connection
	opts := badger.DefaultOptions("./data")
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()




	broker := flag.String("broker", "localhost:9092", "The broker address")
	heartbeatTimeout := flag.Duration("heartbeat-timeout", 5*time.Minute, "Duration of inactivity after which a node is considered inactive")
	flag.Parse()
	brokers := []string{*broker}

	// Consumer for Metrics
	metricsConfig := sarama.NewConfig()
	metricsConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	metricLogger := log.New(os.Stdout, "KafkaConsumer: ", log.Ldate|log.Ltime|log.Lshortfile)

	metricsConsumer, err := kafka.NewConsumer(brokers, metricsConfig, metricLogger)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := metricsConsumer.Consumer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// Consumer for heartbeat
	heartbeatConfig := sarama.NewConfig()
	heartbeatConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	heartbeatLogger := log.New(os.Stdout, "KafkaConsumer: ", log.Ldate|log.Ltime|log.Lshortfile)

	heartbeatConsumer, err := kafka.NewConsumer(brokers, heartbeatConfig, heartbeatLogger)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := heartbeatConsumer.Consumer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// Consumer for register
	registerConfig := sarama.NewConfig()
	registerConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	registerLogger := log.New(os.Stdout, "KafkaConsumer: ", log.Ldate|log.Ltime|log.Lshortfile)

	registerConsumer, err := kafka.NewConsumer(brokers, registerConfig, registerLogger)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := registerConsumer.Consumer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// Producer for trigger message
	producerConfig := sarama.NewConfig()
	producer, err := kafka.NewProducer([]string{"localhost:9092"}, producerConfig, log.New(os.Stdout, "KafkaProducer: ", log.Ldate|log.Ltime|log.Lshortfile))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := producer.Producer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// Producer for test config message
	testProducerConfig := sarama.NewConfig()
	testConfigProducer, err := kafka.NewProducer([]string{"localhost:9092"}, testProducerConfig, log.New(os.Stdout, "KafkaProducer: ", log.Ldate|log.Ltime|log.Lshortfile))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := testConfigProducer.Producer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// Create the orchestrator instance
	orchestrator := orchestrator.NewOrchestrator(
		heartbeatConsumer,
		metricsConsumer,
		registerConsumer,
		producer,
		testConfigProducer,
		*heartbeatTimeout,
		db,
	)

	// Wait group for the register consumer
	var wg sync.WaitGroup
	wg.Add(1)
	go orchestrator.RunRegisterConsumer(&wg)
	wg.Wait()


	// Create a new gin router
	router := gin.Default()



	orchestrator.SetupHTTPHandlers(router)

	// Create a signal channel to listen for the interrupt signal.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)


	// Start the HTTP server
	go func() {
		if err := router.Run(":8081"); err != nil {
			log.Fatal(err)
		}
	}()

	// Create a signal channel to listen for the interrupt signal.
	signalChan = make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)


	// Wait for either an interrupt signal or the HTTP server to exit.
	select {
	case <-signalChan:
		fmt.Println("Received interrupt signal. Shutting down...")

		// Additional cleanup or shutdown logic if needed.

		

		// Close the BadgerDB connection.
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}

		// Exit the application.
		os.Exit(0)
	}

}