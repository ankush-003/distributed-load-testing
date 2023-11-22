package orchestrator

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
	"tester/kafka"
	"github.com/dgraph-io/badger/v3"
)


type MetricsResponse struct {
	Metrics []kafka.MetricsMessage `json:"metrics"`
}

// RetrieveAllNodesEndpoint retrieves all registered nodes.
func RetrieveAllNodesEndpoint(c *gin.Context, db *badger.DB) {
	var registeredNodes []kafka.RegisterMessage

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("register"))
		if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		err = json.Unmarshal(val, &registeredNodes)
		return err
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, registeredNodes)
}


func TriggerLoadTestEndpoint(c *gin.Context, orchestrator *Orchestrator) {
	var requestData struct {
		TestType              string `json:"test_type" binding:"required"`
		TestServer             string `json:"test_server"`
		TestMessageDelay      int    `json:"test_message_delay" binding:"required"`
		MessageCountPerDriver int    `json:"message_count_per_driver" binding:"required"`
	}

	// Bind JSON request body to the struct
	if err := c.ShouldBindJSON(&requestData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Trigger the load test with the provided parameters
	orchestrator.TriggerLoadTestFromAPI(requestData.TestType,requestData.TestServer, requestData.TestMessageDelay, requestData.MessageCountPerDriver)

	c.JSON(http.StatusOK, gin.H{"message": "Load test triggered successfully"})
}


func RetrieveMetricsForNodeEndpoint(c *gin.Context, db *badger.DB) {
	nodeID := c.Param("nodeid")
	var retrievedMetrics kafka.MetricsMessage

	err := db.View(func(txn *badger.Txn) error {
		key := []byte("metrics:" + nodeID)
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		err = json.Unmarshal(val, &retrievedMetrics)
		return err
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, retrievedMetrics)
}



// RetrieveAllMetricsEndpoint retrieves metrics for all registered nodes.
func RetrieveAllMetricsEndpoint(c *gin.Context, orchestrator *Orchestrator) {
	orchestrator.mu.Lock()
	defer orchestrator.mu.Unlock()

	var allMetrics []kafka.MetricsMessage

	// Loop through all registered nodes and retrieve metrics
	for nodeID := range orchestrator.driverNodes {
		key := []byte("metrics:" + nodeID)

		// Retrieve the metrics for the current node ID
		err := orchestrator.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}

			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			var metrics kafka.MetricsMessage
			err = json.Unmarshal(val, &metrics)
			if err != nil {
				return err
			}

			allMetrics = append(allMetrics, metrics)
			return nil
		})

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	// Create a response containing all metrics
	response := MetricsResponse{
		Metrics: allMetrics,
	}

	c.JSON(http.StatusOK, response)
}


func RetrieveHeartbeatEndpoint(c *gin.Context, db *badger.DB) {
	nodeID := c.Param("nodeid")
	var retrievedHeartbeat []kafka.HeartbeatMessage

	err := db.View(func(txn *badger.Txn) error {
		key := []byte("heartbeat:" + nodeID)
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		err = json.Unmarshal(val, &retrievedHeartbeat)
		return err
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, retrievedHeartbeat)
}


// RetrieveTestConfigEndpoint retrieves test config data.
func RetrieveTestConfigEndpoint(c *gin.Context, db *badger.DB) {
	var retrievedMessages []kafka.TestConfigMessage

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("testConfigMessages"))
		if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		err = json.Unmarshal(val, &retrievedMessages)
		return err
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, retrievedMessages)
}

// SetupHTTPHandlers configures the HTTP routes.
func (o *Orchestrator) SetupHTTPHandlers(router *gin.Engine) {
	router.GET("/all-nodes", func(c *gin.Context) {
		RetrieveAllNodesEndpoint(c, o.db)
	})

	router.POST("/trigger-load-test", func(c *gin.Context) {
		TriggerLoadTestEndpoint(c, o)
	})

	router.GET("/test-config", func(c *gin.Context) {
		RetrieveTestConfigEndpoint(c, o.db)
	})

	router.GET("/metrics/:nodeid", func(c *gin.Context) {
		RetrieveMetricsForNodeEndpoint(c, o.db)
	})

	router.GET("/all-metrics", func(c *gin.Context) {
		RetrieveAllMetricsEndpoint(c, o)
	})

	router.GET("/heartbeat/:nodeid", func(c *gin.Context) {
		RetrieveHeartbeatEndpoint(c, o.db)
	})
}
