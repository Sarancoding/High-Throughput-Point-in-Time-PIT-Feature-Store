# Setup Guide

This guide covers detailed environment configuration for Kafka, Flink, ClickHouse, and Prometheus.

## Kafka Configuration

### Broker Settings
```yaml
# docker-compose.yml
kafka:
  KAFKA_BROKER_ID: 1
  KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
  KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

### Topic Creation
```bash
# Create CDC events topic
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic cdc-events \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=604800000

# Create feature output topic
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic feature-updates \
  --partitions 3 \
  --replication-factor 1
```

## Flink Configuration

### Checkpointing Settings
```python
# stream/flink_job.py
env.enable_checkpointing(
    interval=60000,  # 1 minute
    mode=CheckpointingMode.EXACTLY_ONCE
)
env.get_checkpoint_config().set_min_pause_between_checkpoints(30000)
env.get_checkpoint_config().set_checkpoint_timeout(600000)
env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
```

### State Backend
```python
env.set_state_backend(RocksDBStateBackend('hdfs://namenode:8020/flink-checkpoints'))
```

### Watermark Strategy
```python
# Allow 5 seconds lateness
WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
```

## ClickHouse Configuration

### Server Settings
```xml
<!-- config.xml -->
<clickhouse>
    <logger level="information"/>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <max_connections>4096</max_connections>
    <keep_alive_timeout>3000</keep_alive_timeout>
    
    <!-- MergeTree settings -->
    <merge_tree>
        <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
        <parts_to_throw_insert>300</parts_to_throw_insert>
    </merge_tree>
</clickhouse>
```

### Table Engine Configuration
```sql
-- ReplacingMergeTree for deduplication
CREATE TABLE features_raw (
    account_id String,
    feature_name String,
    event_time DateTime64(3),
    processing_time DateTime64(3),
    value Float64,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_time)
ORDER BY (account_id, feature_name, event_time);

-- AggregatingMergeTree for pre-computed aggregations
CREATE TABLE features_aggregated (
    account_id String,
    window_start DateTime64(3),
    window_end DateTime64(3),
    feature_sum SimpleAggregateFunction(sum, Float64),
    feature_count SimpleAggregateFunction(count, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (account_id, window_start);
```

## Prometheus Configuration

### Scrape Config
```yaml
# prometheus/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'flink'
    static_configs:
      - targets: ['flink-jobmanager:8081']
    metrics_path: '/metrics'
    
  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka-exporter:9308']
      
  - job_name: 'clickhouse'
    static_configs:
      - targets: ['clickhouse-exporter:9363']
```

### Alert Rules
```yaml
# prometheus/alerts.yml
groups:
  - name: pit-feature-store
    rules:
      - alert: HighIngestionLag
        expr: kafka_consumer_lag > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High Kafka consumer lag detected"
          
      - alert: FlinkCheckpointFailure
        expr: flink_checkpoints_failed > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Flink checkpoint failed"
```

## Grafana Dashboard Import

```bash
# Import dashboards via API
curl -X POST \
  -H "Content-Type: application/json" \
  -d @observability/grafana_dashboards.json \
  http://admin:admin@localhost:3000/api/dashboards/db
```

## Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BROKERS` | Kafka broker addresses | `localhost:9092` |
| `CLICKHOUSE_HOST` | ClickHouse server URL | `http://localhost:8123` |
| `FLINK_JOBMANAGER` | Flink JobManager URL | `http://localhost:8081` |
| `PROMETHEUS_ENDPOINT` | Prometheus metrics endpoint | `http://localhost:9090` |
| `ENCRYPTION_KEY` | AES-256 encryption key | (required) |
| `CHECKPOINT_DIR` | Flink checkpoint directory | `/tmp/flink-checkpoints` |

## Security Configuration

### SSL/TLS for Kafka
```properties
# producer.properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="producer" password="secret";
ssl.truststore.location=/etc/kafka/ssl/truststore.jks
ssl.truststore.password=truststore-password
```

### ClickHouse User Management
```sql
CREATE USER pit_user IDENTIFIED BY sha256_password;
GRANT SELECT ON pit_feature_store.* TO pit_user;
GRANT INSERT ON pit_feature_store.features_raw TO pit_user;
```

## Performance Tuning

### Kafka Producer Optimization
```python
producer_config = {
    'bootstrap.servers': 'broker1:9092,broker2:9092',
    'acks': 'all',
    'retries': 5,
    'batch.size': 16384,
    'linger.ms': 5,
    'compression.type': 'lz4',
    'enable.idempotence': True
}
```

### ClickHouse Query Optimization
```sql
-- Add projections for faster PIT queries
ALTER TABLE features_raw ADD PROJECTION pit_projection (
    SELECT account_id, feature_name, argMax(value, event_time) as latest_value
    GROUP BY account_id, feature_name
);
```
