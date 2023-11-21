package main

import (
	"net/http"
	ginMonitor "github.com/bancodobrasil/gin-monitor"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	// Initialize Monitoring
	monitor, err := ginMonitor.New(
		"v1.0.0",
		ginMonitor.DefaultErrorMessageKey,
		ginMonitor.DefaultBuckets,
	)
	if err != nil {
		panic(err)
	}

	r := gin.Default()

	// Register mux-monitor middleware
	r.Use(monitor.Prometheus())
	// Register metrics endpoint
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Ping Endpoint
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	// Metrics Endpoint

	r.Run() // listen and serve on 0.0.0.0:8080 (for windows "localhost:8080")
}