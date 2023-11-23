# Distributed Load Testing System
An orchestrated load-testing system designed to coordinate multiple driver nodes for executing highly concurrent, high-throughput load tests on a web server. This system employs Kafka as the underlying communication service.

## Architecture

![Architecture](https://github.com/ankush-003/distributed-load-testing/assets/94037471/447ce4ab-b7b0-4249-9d58-6b23d4ee4f22)
- The orchestrator node and driver node are implemented as separate processes
- The Orchestrator node and the Driver node communicate via Kafka

## Installing Go & Kafka
Head over to the [Installation Files](https://github.com/ankush-003/distributed-load-testing/tree/main/installation%20files) and execute the following commands to install Go and Kafka
```bash
sudo chmod a+x *.sh
source go.sh
source kafka.sh
```
## Setting up Fastapi Dashboard
```bash
pip install fastapi[all] httpx
python3 app.py
```
## Running Driver & Orchestrator
install all dependencies
```bash
go get .
```
run test server
```bash
go build httpServer.go
./httpServer
```
build and run orchestrator
```bash
go build Orchestra.go
./Orchestra
```
build and run driver node (on multiple terminals as different processes
```bash
go build driverNode.go
./driverNode
```
## Features to be implemented
- [x] Metrics Dashboard
- [ ] Persistent Driver Nodes 
- [ ] Containerisation using Docker
## Screenshots
<div style="display: flex;">
  <img src="https://github.com/ankush-003/distributed-load-testing/assets/94037471/0165e0a6-694c-4e83-9207-83cd39f112d0" alt="Screenshot from 2023-11-22 15-58-35">
  <img src="https://github.com/ankush-003/distributed-load-testing/assets/94037471/8c1bebdb-c00a-4fac-8d1b-37920400ba16" alt="Screenshot from 2023-11-22 15-57-58">
</div>

![Screenshot from 2023-11-22 15-58-11](https://github.com/ankush-003/distributed-load-testing/assets/94037471/f5f1daf8-fdbd-46c7-bffb-b6450391c59a)
