# Installation Guide

This guide provides step-by-step instructions for installing and configuring the High-Throughput Point-in-Time (PIT) Feature Store.

## Prerequisites

- Python 3.9+
- Docker and Docker Compose
- Apache Kafka 2.8+
- Apache Flink 1.14+
- ClickHouse 21.8+
- Prometheus 2.30+
- Grafana 8.0+

## Step 1: Clone the Repository

```bash
git clone https://github.com/Sarancoding/High-Throughput-Point-in-Time-PIT-Feature-Store.git
cd High-Throughput-Point-in-Time-PIT-Feature-Store
```

## Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

## Step 3: Configure Environment Variables

Copy the example environment file and update with your credentials:

```bash
cp harness/.env.example .env
```

Update the following variables in `.env`:
- `KAFKA_BROKERS`: Your Kafka broker addresses
- `CLICKHOUSE_HOST`: Your ClickHouse server URL
- `FLINK_CHECKPOINT_DIR`: Directory for Flink checkpoints
- `PROMETHEUS_ENDPOINT`: Prometheus metrics endpoint
- `ENCRYPTION_KEY`: AES-256 encryption key (32 bytes)

## Step 4: Start Infrastructure Services

```bash
docker-compose up -d
```

This starts:
- Kafka brokers
- Zookeeper
- ClickHouse server
- Prometheus
- Grafana

## Step 5: Initialize ClickHouse Schema

```bash
python storage/clickhouse_schema.py
```

## Step 6: Deploy Flink Job

```bash
flink run -py stream/flink_job.py
```

## Step 7: Verify Installation

Run the test suite to verify everything is working:

```bash
pytest tests/
```

## Troubleshooting

### Kafka Connection Issues
Ensure Kafka brokers are accessible and the `KAFKA_BROKERS` variable is correct.

### ClickHouse Schema Errors
Check that ClickHouse is running and the connection URL is valid.

### Flink Job Failures
Review Flink logs at `checkpoints/flink.log` for detailed error messages.
