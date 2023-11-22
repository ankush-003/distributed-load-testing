package orchestrator

import (
	"encoding/json"
	"fmt"
	"log"
	// "os"
	"time"
	"sync"
	"github.com/dgraph-io/badger/v3"
	"github.com/google/uuid"
	"tester/kafka"
)

type Orchestrator struct {
	mu                sync.Mutex
	driverNodes       map[string]time.Time
	heartbeatConsumer *kafka.Consumer
	metricsConsumer   *kafka.Consumer
	registerConsumer  *kafka.Consumer
	testConfigProducer *kafka.Producer
	triggerProducer    *kafka.Producer
	db                *badger.DB
	heartbeatTimeout  time.Duration
}


// NewOrchestrator initializes a new Orchestrator instance.
func NewOrchestrator(heartbeatConsumer *kafka.Consumer, metricsConsumer *kafka.Consumer, registerConsumer *kafka.Consumer, testConfigProducer *kafka.Producer, triggerProducer *kafka.Producer, heartbeatTimeout time.Duration, db *badger.DB) *Orchestrator {
	return &Orchestrator{
		driverNodes:       make(map[string]time.Time),
		heartbeatConsumer: heartbeatConsumer,
		metricsConsumer:   metricsConsumer,
		registerConsumer:  registerConsumer,
		testConfigProducer: testConfigProducer,
		triggerProducer:    triggerProducer,
		db:                db,
		heartbeatTimeout:  heartbeatTimeout,
	}
}


func (o *Orchestrator) RunMetricsConsumer() {
	messageChan := make(chan kafka.MetricsMessage)
	doneChan := make(chan struct{})

	go o.metricsConsumer.ConsumeMetricsMessages("metrics-topic", messageChan, doneChan)

ConsumerLoop:
	for {
		select {
		case msg := <-messageChan:
			
			o.handleMetrics(msg)
		case <-doneChan:
			break ConsumerLoop
		}
	}
}

func (o *Orchestrator) RunRegisterConsumer(wg *sync.WaitGroup) {
	messageChan := make(chan kafka.RegisterMessage)
	doneChan := make(chan struct{})

	go o.registerConsumer.ConsumeRegisterMessages("register-topic", messageChan, doneChan,8)

ConsumerLoop:
	for {
		select {
		case msg := <-messageChan:
			o.handleRegister(msg)
			if err := o.logRegister(msg); err != nil {
				log.Printf("Error logging register to BadgerDB: %v", err)
			}
		case <-doneChan:
			
			break ConsumerLoop
		}
	}
	wg.Done()
}

func (o *Orchestrator) RunHeartbeatConsumer() {
	messageChan := make(chan kafka.HeartbeatMessage)
	doneChan := make(chan struct{})

	go o.heartbeatConsumer.ConsumeHeartbeatMessages("heartbeat", messageChan, doneChan)

ConsumerLoop:
	for {
		select {
		case msg := <-messageChan:
			o.handleHeartbeat(msg)
			// Log heartbeat details to BadgerDB
			if err := o.logHeartbeatToBadger(msg); err != nil {
				log.Printf("Error logging heartbeat to BadgerDB: %v", err)
			}
		case <-doneChan:
			break ConsumerLoop
		}
	}
}
func (o *Orchestrator) handleMetrics(metrics kafka.MetricsMessage) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Check if the node is registered before processing metrics
	if _, ok := o.driverNodes[metrics.NodeID]; !ok {
		fmt.Printf("Ignoring metrics from unregistered node: %s\n", metrics.NodeID)
		return
	}

	//printing the node id and metrics received
	fmt.Printf("Node ID: %s\n", metrics.NodeID)
	fmt.Printf("Metrics Received: %v\n", metrics)

	

	// Marshal the MetricsMessage into a JSON-encoded byte slice
	metricsMessageJSON, err := json.Marshal(metrics)
	if err != nil {
		log.Fatal(err)
	}

	// Set the byte slice as the value for a key in the BadgerDB instance.
	err = o.db.Update(func(txn *badger.Txn) error {
		key := []byte("metrics:" + metrics.NodeID)
		err := txn.Set(key, metricsMessageJSON)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}		
}

func (o *Orchestrator) handleRegister(register kafka.RegisterMessage) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Update the registered nodes map
	o.driverNodes[register.NodeID] = time.Now()
	fmt.Printf("Node registered: %s\n", register.NodeID)

}




func (o *Orchestrator) handleHeartbeat(heartbeat kafka.HeartbeatMessage) {
	o.mu.Lock()
	defer o.mu.Unlock()

	log.Println("Heartbeat Received:", heartbeat)

	// Check if the node is registered before processing heartbeat
	if _, ok := o.driverNodes[heartbeat.NodeID]; !ok {
		fmt.Printf("Ignoring heartbeat from unregistered node: %s\n", heartbeat.NodeID)
		return
	}

	// Update the last activity time for the registered node
	o.driverNodes[heartbeat.NodeID] = time.Now()
}


func (o *Orchestrator) logRegister(register kafka.RegisterMessage) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Retrieve existing heartbeat messages
	var existingRegisterMessages []kafka.RegisterMessage


	err := o.db.View(func(txn *badger.Txn) error {
		key := []byte("register")

		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				// Key not found, initialize an empty array
				return nil
			}
			return err
		}

		// Retrieve the value directly using item.ValueCopy
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		// Unmarshal the existing values into existingHeartbeatMessages
		err = json.Unmarshal(val, &existingRegisterMessages)
		return err
	})

	if err != nil {
		return err
	}

	// Append the new heartbeat message to existing messages
	existingRegisterMessages = append(existingRegisterMessages, register)

	// Marshal the combined array into a JSON-encoded byte slice
	combinedRegisterMessagesJSON, err := json.Marshal(existingRegisterMessages)
	if err != nil {
		return err
	}

	// Set the JSON-encoded heartbeat messages as the value for the key in BadgerDB
	err = o.db.Update(func(txn *badger.Txn) error {
		key := []byte("register")
		err := txn.Set(key, combinedRegisterMessagesJSON)
		return err
	})

	return err

}



func (o *Orchestrator) logHeartbeatToBadger(heartbeat kafka.HeartbeatMessage) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Retrieve existing heartbeat messages
	var existingHeartbeatMessages []kafka.HeartbeatMessage

	err := o.db.View(func(txn *badger.Txn) error {
		key := []byte("heartbeat:" + heartbeat.NodeID)

		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				// Key not found, initialize an empty array
				return nil
			}
			return err
		}

		// Retrieve the value directly using item.ValueCopy
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		// Unmarshal the existing values into existingHeartbeatMessages
		err = json.Unmarshal(val, &existingHeartbeatMessages)
		return err
	})

	if err != nil {
		return err
	}

	// Append the new heartbeat message to existing messages
	existingHeartbeatMessages = append(existingHeartbeatMessages, heartbeat)

	// Marshal the combined array into a JSON-encoded byte slice
	combinedHeartbeatMessagesJSON, err := json.Marshal(existingHeartbeatMessages)
	if err != nil {
		return err
	}

	// Set the JSON-encoded heartbeat messages as the value for the key in BadgerDB
	err = o.db.Update(func(txn *badger.Txn) error {
		key := []byte("heartbeat:" + heartbeat.NodeID)
		err := txn.Set(key, combinedHeartbeatMessagesJSON)
		return err
	})

	return err
}


func (o *Orchestrator) TriggerLoadTestFromAPI(testType string,TestServer string, testMessageDelay, messageCountPerDriver int) {
	// Additional logic to determine when to trigger the load test.
	// For now, trigger the test immediately.

	// Generating a random test id
	testID := uuid.New().String()

	testConfigMessages := []kafka.TestConfigMessage{{
		TestID:                testID,
		TestType:              testType,
		TestServer: TestServer,
		TestMessageDelay:      testMessageDelay,
		MessageCountPerDriver: messageCountPerDriver,
	}}

	_, errors := o.testConfigProducer.ProduceTestConfigMessages("test-config-topic", testConfigMessages)

	if errors != 0 {
		log.Fatalf("Error producing test config message: %v", errors)
	}

	// Storing test config data in BadgerDB
	testConfigMessagesJSON, err := json.Marshal(testConfigMessages)
	if err != nil {
		log.Fatal(err)
	}

	// Set the byte slice as the value for a key in the BadgerDB instance.
	err = o.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte("testConfigMessages"), testConfigMessagesJSON)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}

	// Sleep for a short duration to simulate additional logic
	time.Sleep(5 * time.Second)

	// Trigger message
	trigMessage := kafka.TriggerMessage{
		TestID:  testID,
		Trigger: "YES",
	}

	_, terrors := o.triggerProducer.ProduceTriggerMessages("trigger-topic", []kafka.TriggerMessage{trigMessage})

	if terrors != 0 {
		log.Fatalf("Error producing trigger message: %v", terrors)
	}
}
