package driver

import (
  "github.com/dgraph-io/badger/v3"
  "github.com/ankush-003/distributed-load-testing/kafka"
  "log"
  "sort"
	//"os"
	//"os/signal"
	//"sync"
	"time"
  "github.com/google/uuid"	
  "fmt"
)

type DriverNode struct {
	NodeID   string
	NodeIP   string
	TestID   string
	TestType string
	TestServer string
  MessageCountPerDriver int
  TestMessageDelay int
}

type MetricsStore struct {
	Db *badger.DB
}

func NewMetricsStore() (*MetricsStore, error) {
	opts := badger.DefaultOptions("").WithInMemory(true) // You can adjust BadgerDB options here
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &MetricsStore{Db: db}, nil
}

func (m *MetricsStore) StoreLatency(requestIndex int, latency time.Duration) error {
	key := []byte(fmt.Sprintf("request-%d", requestIndex))

	err := m.Db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, []byte(latency.String()))
		return err
	})
	return err
}

func (m *MetricsStore) GetLatency(requestIndex int) (time.Duration, error) {
	key := []byte(fmt.Sprintf("request-%d", requestIndex))
	var latency time.Duration

	err := m.Db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			latency, err = time.ParseDuration(string(val))
			return err
		})
		return err
	})

	if err != nil {
		return 0, err
	}

	return latency, nil
}

func getMetrics(latencies []time.Duration) (string, string, string, string) {
	if len(latencies) == 0 {
		return "", "", "", ""
	}

	// Sort the latencies
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Calculate mean
	var sum time.Duration
	for _, latency := range latencies {
		sum += latency
	}
	mean := time.Duration(int(sum) / len(latencies))

	// Calculate median
	var median time.Duration
	if len(latencies)%2 == 0 {
		median = (latencies[len(latencies)/2-1] + latencies[len(latencies)/2]) / 2
	} else {
		median = latencies[len(latencies)/2]
	}

	// Calculate min and max
	min := latencies[0]
	max := latencies[len(latencies)-1]

	// Format results as strings
	return mean.String(), median.String(), min.String(), max.String()
}

func (m *MetricsStore) CalculateMetrics(logger *log.Logger) (string, string, string, string) {
	var latencies []time.Duration

	err := m.Db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			latency, err := time.ParseDuration(string(valCopy))
			if err != nil {
				return err
			}
			latencies = append(latencies, latency)
		}
		return nil
	})

	if err != nil {
		logger.Println("Error fetching latencies:", err)
		return "", "", "", ""
	}

	return getMetrics(latencies)
}

func (m *MetricsStore) ProduceMetricsToTopic(done <-chan struct{}, producer *kafka.Producer, topic string, driverNode *DriverNode, logger *log.Logger) {
	ticker := time.NewTicker(10 * time.Millisecond) // Adjust the interval for sending metrics
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			meanLatency, medianLatency, minLatency, maxLatency := m.CalculateMetrics(logger)

			// Create MetricsData object
			metricsData := kafka.MetricsData{
				MeanLatency:   meanLatency,
				MedianLatency: medianLatency,
				MinLatency:    minLatency,
				MaxLatency:    maxLatency,
			}

			// Produce metrics message to Kafka
			metricsMsg := kafka.MetricsMessage{
				NodeID:  driverNode.NodeID,
				TestID:  driverNode.TestID,
				ReportID: uuid.New().String(),
				Metrics: metricsData,
			}
			producer.ProduceMetricsMessages(topic, []kafka.MetricsMessage{metricsMsg})
      logger.Println("Metrics Produced:", metricsMsg)
		case <-done:
			return // Stop producing metrics when done signal is received
		}
	}
}

func (m *MetricsStore) ProduceMetricsToTopicOnce(producer *kafka.Producer, topic string, driverNode *DriverNode, logger *log.Logger) {
	meanLatency, medianLatency, minLatency, maxLatency := m.CalculateMetrics(logger)

	// Create MetricsData object
	metricsData := kafka.MetricsData{
		MeanLatency:   meanLatency,
		MedianLatency: medianLatency,
		MinLatency:    minLatency,
		MaxLatency:    maxLatency,
	}

	// Produce metrics message to Kafka
	metricsMsg := kafka.MetricsMessage{
		NodeID:   driverNode.NodeID,
		TestID:   driverNode.TestID,
		ReportID: uuid.New().String(),
		Metrics:  metricsData,
	}

	producer.ProduceMetricsMessages(topic, []kafka.MetricsMessage{metricsMsg})
	logger.Println("Metrics Produced:", metricsMsg)
}