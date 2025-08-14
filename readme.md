# Hydroponic Monitoring Project

This project runs a fully containerized hydroponic monitoring stack:
- **Kafka** for message transport
- **Spark** (producer & consumer) for simulation and analytics
- **InfluxDB** for time-series data
- **Prometheus** for metrics
- **Grafana** for visualization

## Prerequisites

- Docker Engine
- Docker Compose

## Structure

```
.
├── producer.py
├── consumer.py
├── requirements.txt
├── Dockerfile-producer
├── Dockerfile-consumer
├── docker-compose.yml
├── prometheus.yml
└── README.md
```

## Running the stack

1. Clone or unzip project folder:
   ```bash
   cd bulletproof_hydroponic_project
   ```

2. Build & start containers:
   ```bash
   docker-compose up --build -d
   ```

2(b). Build & start containers:
   ```bash
   docker-compose up -d
   ```

3. Confirm services are up:
   ```bash
   docker-compose ps
   ```

## Access UIs

- **Grafana**: http://localhost:3000 (admin/admin)  
- **Prometheus**: http://localhost:9090  
- **InfluxDB**: http://localhost:8086 (admin/password123)  
- **Spark Producer Metrics**: http://localhost:8000/metrics  
- **Spark Consumer Metrics**: http://localhost:8001/metrics  
- **Kafka Broker**: PLAINTEXT://localhost:9092  

## Viewing Logs

- **Producer logs**:
  ```bash
  docker logs spark-producer -f
  ```
- **Consumer logs**:
  ```bash
  docker logs spark-consumer -f
  ```
- **Kafka logs**:
  ```bash
  docker logs kafka -f
  ```

## Stopping

```bash
docker-compose down
```

