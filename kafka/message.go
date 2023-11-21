package kafka

type RegisterMessage struct {
  NodeID      string `json:"node_id"`
  NodeIP      string `json:"node_IP"`
  MessageType string `json:"message_type"`
}

type TestConfigMessage struct {
  TestID                 string `json:"test_id"`
  TestType               string `json:"test_type"`
  TestMessageDelay       int `json:"test_message_delay"`
  MessageCountPerDriver  int `json:"message_count_per_driver"`
}

type TriggerMessage struct {
  TestID  string `json:"test_id"`
  Trigger string `json:"trigger"`
}

type MetricsMessage struct {
  NodeID    string `json:"node_id"`
  TestID    string `json:"test_id"`
  ReportID  string `json:"report_id"`
  Metrics   MetricsData `json:"metrics"`
}

type MetricsData struct {
  MeanLatency   string `json:"mean_latency"`
  MedianLatency string `json:"median_latency"`
  MinLatency    string `json:"min_latency"`
  MaxLatency    string `json:"max_latency"`
}

type HeartbeatMessage struct {
  NodeID    string `json:"node_id"`
  Heartbeat string `json:"heartbeat"`
  Timestamp string `json:"timestamp"`
}




