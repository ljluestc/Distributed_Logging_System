## Go implementation of Distributed Logging System

This `golang/` folder contains a Go reimplementation of the Python system:
- Microservices (Inventory, Order, Payment) emit:
  - registrations to Kafka topic `microservice_registration`
  - periodic heartbeats to `microservice_heartbeats`
  - random logs (INFO/WARN/ERROR) to `microservice_logs`
  - logs are also sent to Fluentd
- Central consumer reads all three topics, prints colored output, tracks node liveness, and indexes logs into Elasticsearch index `microservice_logs`.

### Prerequisites
- Go 1.22+
- Kafka reachable at `192.168.222.127:9092` (adjust in code if needed)
- Fluentd reachable at the same host on port `24224`
- Elasticsearch reachable at `http://localhost:9200`

### Module setup
From the repository root:

```bash
cd golang
go mod tidy
```

If you prefer a different module path than `distributed_logging_system`, update `golang/go.mod` accordingly before running `go mod tidy`.

### Running microservices
Open separate terminals for each service:

```bash
cd golang
go run ./microservices/cmd/inventory
```

```bash
cd golang
go run ./microservices/cmd/order
```

```bash
cd golang
go run ./microservices/cmd/payment
```

Each service will:
- Register once on startup
- Send a heartbeat every 5 seconds
- Emit a random log every 3 seconds (INFO/WARN/ERROR)

### Running central consumer

```bash
cd golang
go run ./central/cmd/consumer
```

The consumer:
- Prints colored logs with extra info for WARN/ERROR
- Tracks node registrations and disconnections (timeout: 10s since last heartbeat)
- Indexes logs into Elasticsearch index `microservice_logs`

### Configuration
Default connection settings are in:
- `golang/microservices/config/config.go`
- `golang/central/cmd/consumer/main.go` (bootstrap and ES address)

Adjust host/ports to match your environment.



