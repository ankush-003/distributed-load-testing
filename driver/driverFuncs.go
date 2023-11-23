package driver

import (
	"github.com/ankush-003/distributed-load-testing/kafka"
	"time"
	"log"
	"sync"
	"net/http"
)

func WaitForTestConfig(testConfigTopic string, triggerTopic string, consumer *kafka.Consumer, testConfigChan chan<- kafka.TestConfigMessage, triggerReceived chan struct{}, logger *log.Logger) {
	testConfigMsgChan := make(chan kafka.TestConfigMessage)
	doneChan := make(chan struct{})

	go func() {
		defer close(testConfigMsgChan)
		defer close(doneChan)
		consumer.ConsumeTestConfigAndTriggerMessages(testConfigTopic, triggerTopic, testConfigMsgChan, triggerReceived)
	}()

	select {
	case testConfigMsg := <-testConfigMsgChan:
		testConfigChan <- testConfigMsg
	case <-time.After(1 * time.Minute):
		logger.Println("No test config message received within the timeout")
	}
}

func HandleTestConfig(testConfigMsg kafka.TestConfigMessage, driverNode *DriverNode, logger *log.Logger) {
	// Handle test configuration message
	//logger.Printf("Received test config message: %+v\n", testConfigMsg)

	// Implement logic for handling test configuration
	driverNode.TestID = testConfigMsg.TestID
	driverNode.TestServer = testConfigMsg.TestServer
	driverNode.TestType = testConfigMsg.TestType
	driverNode.MessageCountPerDriver = testConfigMsg.MessageCountPerDriver
	driverNode.TestMessageDelay = testConfigMsg.TestMessageDelay
	logger.Println("Received Test Config!")
	logger.Println("Driver Node Info:", driverNode)
}

func HandleTrigger(metricsTopic string,driverNode *DriverNode, testConfigMsg *kafka.TestConfigMessage, producer *kafka.Producer, metricsStore *MetricsStore, logger *log.Logger) {
	done := make(chan struct{})

	// Start a goroutine for continuous metrics calculation and sending
	go func() {
		//defer close(done)
		metricsStore.ProduceMetricsToTopic(done, producer, metricsTopic, driverNode, logger)
	}()
	
	if driverNode.TestType == "AVALANCHE" {
		logger.Println("Starting Load Test!")
		AvalancheTesting(driverNode.TestServer, metricsStore, driverNode.MessageCountPerDriver, done, logger)
		metricsStore.ProduceMetricsToTopicOnce(producer, metricsTopic, driverNode, logger)
	} else if driverNode.TestType == "TSUNAMI" {
		logger.Println("Starting Load Test!")
		TsunamiTesting(driverNode.TestServer, metricsStore, driverNode.TestMessageDelay, driverNode.MessageCountPerDriver, done, logger)
		metricsStore.ProduceMetricsToTopicOnce(producer, metricsTopic, driverNode, logger)
	} else {
		logger.Panic("Invalid Test Type")
	}
}

func AvalancheTesting(ServerURL string, metricsStore *MetricsStore, requestCount int, done chan struct{}, logger *log.Logger) {
	var wg sync.WaitGroup

	// Sending concurrent HTTP requests
	for i := 0; i < requestCount; i++ {
		wg.Add(1)
		go func(reqNum int) {
			defer wg.Done()
			SendHTTPRequest(ServerURL, reqNum, metricsStore, logger)
		}(i)
	}

	wg.Wait() // Wait for all requests to be sent

	// When testing is completed, signal to stop metrics calculation and sending
	close(done)
	logger.Println("Avalanche testing completed")
	log.Println("Avalanche testing completed")
}

func TsunamiTesting(ServerURL string, metricsStore *MetricsStore, interval int, requestCount int, done chan struct{}, logger *log.Logger) {
	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer ticker.Stop()

	for i := 1; i <= requestCount; i++ {
		select {
		case <-ticker.C:
			SendHTTPRequest(ServerURL, i, metricsStore, logger)
		case <-done:
			return
		}
	}

	// When testing is completed, signal to stop metrics calculation and sending
	close(done)
	logger.Println("Tsunami testing completed")
	log.Println("Tsunami testing completed")
}

func SendHTTPRequest(url string, requestNumber int, metricsStore *MetricsStore, logger *log.Logger) {
	start := time.Now()
	resp, err := http.Get(url)
	if err != nil {
		logger.Printf("Error making request: %s\n", err)
		return
	}
	defer resp.Body.Close()

	duration := time.Since(start)

	// Store latency in MetricsStore with request number
	if err := metricsStore.StoreLatency(requestNumber, duration); err != nil {
		logger.Printf("Error storing latency for request %d: %s\n", requestNumber, err)
		return
	}

	logger.Printf("Response Status: %s, Latency for request %d: %v\n", resp.Status, requestNumber, duration)
}

func SendHeartbeats(heartbeatTopic string,driverNode *DriverNode, producer *kafka.Producer, done <-chan struct{}, logger *log.Logger) {
	ticker := time.NewTicker(10 * time.Second) // Create a ticker for a 10-second interval
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C: // Wait for the ticker to signal the interval
			heartbeatMsg := kafka.HeartbeatMessage{
				NodeID:    driverNode.NodeID,
				Heartbeat: "YES",
				Timestamp: time.Now().Format(time.RFC3339),
			}
			producer.ProduceHeartbeatMessagesPartitions(heartbeatTopic, []kafka.HeartbeatMessage{heartbeatMsg})
			//logger.Println("heartbeat:", heartbeatMsg)

		case <-done:
			return // Stop sending heartbeats when done signal is received
		}
	}
}