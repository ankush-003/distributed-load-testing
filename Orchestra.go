package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"github.com/ankush-003/distributed-load-testing/kafka"
	"time"
	"github.com/IBM/sarama"
	"github.com/dgraph-io/badger/v3"
	"github.com/gin-gonic/gin"
	"github.com/ankush-003/distributed-load-testing/orchestrator"
)


func main() {

	opts := badger.DefaultOptions("./dato")
db, err := badger.Open(opts)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Clear a register item
err = db.Update(func(txn *badger.Txn) error {
    key := []byte("register")
    return txn.Delete(key)
})

if err != nil {
	log.Println("Register need not be deleted as it does not exist")
    log.Fatal(err)
}




	broker := flag.String("broker", "localhost:9092", "The broker address")
	heartbeatTimeout := flag.Duration("heartbeat-timeout", 5*time.Minute, "Duration of inactivity after which a node is considered inactive")
	numDrivers := flag.Int("num-drivers", 2, "Number of driver nodes to expect")
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
	producer, err := kafka.NewProducer(brokers, producerConfig, log.New(os.Stdout, "KafkaProducer: ", log.Ldate|log.Ltime|log.Lshortfile))
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
	testConfigProducer, err := kafka.NewProducer(brokers, testProducerConfig, log.New(os.Stdout, "KafkaProducer: ", log.Ldate|log.Ltime|log.Lshortfile))
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
	go orchestrator.RunRegisterConsumer(&wg, *numDrivers)
	wg.Wait()


	// Start the Kafka metrics, heartbeat, and register consumers.
	go orchestrator.RunHeartbeatConsumer()
	go orchestrator.RunMetricsConsumer()
	
	


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

	// Wait for either an interrupt signal or the HTTP server to exit.
	select {
	case <-signalChan:
		fmt.Println("Received interrupt signal. Shutting down...")

		// Close the BadgerDB connection.
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}

		// Exit the application.
		os.Exit(0)
	}

}