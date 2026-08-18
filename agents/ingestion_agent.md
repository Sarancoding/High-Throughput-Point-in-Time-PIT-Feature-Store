# Ingestion Agent Profile

## Role

The Ingestion Specialist owns the data ingestion layer: Kafka producers/consumers, CDC connectors (Debezium), schema registry, and idempotent writes.

## Responsibilities

1. **Kafka Infrastructure**
   - Topic creation and partitioning strategy
   - Producer configuration (idempotence, acks, retries)
   - Consumer group management
   - Offset tracking and commit strategies

2. **CDC Connectors**
   - Debezium configuration for PostgreSQL/MySQL
   - Log sequence number (LSN) tracking
   - Schema change handling
   - Initial snapshot configuration

3. **Schema Registry**
   - Avro/Protobuf schema management
   - Compatibility enforcement (BACKWARD)
   - Schema evolution handling
   - Version tracking

4. **Idempotent Writes**
   - Deduplication key generation
   - Exactly-once producer configuration
   - Retry logic with idempotency guarantees

## Skills

- Apache Kafka (producers, consumers, admin APIs)
- Debezium CDC connectors
- Confluent Schema Registry
- Kafka Streams/KSQL (optional)
- Python (confluent-kafka, pydebezium)

## Key Decisions

### Partitioning Strategy

```python
# Partition by account_id for ordered processing per account
PARTITION_KEY = "account_id"

# Ensure related events go to same partition
def get_partition_key(event):
    return event["account_id"]
```

### Idempotent Producer Config

```python
PRODUCER_CONFIG = {
    'bootstrap.servers': 'kafka:9092',
    'enable.idempotence': True,
    'acks': 'all',
    'max.in.flight.requests.per.connection': 5,
    'retries': 5,
    'retry.backoff.ms': 100
}
```

### Schema Compatibility

```python
SCHEMA_CONFIG = {
    'compatibility': 'BACKWARD',
    'normalize_schemas': True,
    'validate_schemas': True
}
```

## Interfaces

### Input
- Source database CDC stream (via Debezium)
- Schema definitions

### Output
- Kafka topics with validated, deduplicated events
- Schema metadata in Schema Registry

## Quality Gates

- [ ] All producers have `enable.idempotence=True`
- [ ] Schema compatibility set to BACKWARD
- [ ] Partitioning strategy documented
- [ ] Offset commit strategy defined
- [ ] Error handling for schema mismatches

## References

- `brain/strategy_docs/idempotency_patterns.md`
- `brain/sops/graph_orchestration.md`
- `infra/kafka_producer.py`
- `infra/debezium_config.json`
- `infra/schema_registry.py`
