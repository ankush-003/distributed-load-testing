#!/usr/bin/bash

# List Topics
echo "=================================="
echo "          Available Topics	"
echo "=================================="
/usr/local/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create Topics
echo "=================================="
echo "        Creating Given Topic      "
echo "=================================="

/usr/local/kafka/bin/kafka-topics.sh --create --topic $1 --bootstrap-server localhost:9092

# Get topic description
echo "=================================="
echo "        Description of Topic      "  
echo "=================================="
/usr/local/kafka/bin/kafka-topics.sh --describe --topic $1 --bootstrap-server localhost:9092

