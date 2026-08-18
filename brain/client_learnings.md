# Client Learnings & Lessons Learned

## Flink Watermark Configuration

### Lesson: Account for Clock Skew Across CDC Sources

**Problem:** Multiple CDC sources (different databases) had clock skew of up to 3 seconds, causing events to appear "late" even when they arrived on time.

**Solution:** 
```python
# Add buffer to watermark strategy
watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
    Duration.of_seconds(305)  # 5 min + 5 sec clock skew buffer
)
```

**Rule:** Always add 5-10 second buffer to watermark for multi-source CDC pipelines.

---

## ClickHouse AS OF Queries

### Lesson: Explicit event_time Column Required

**Problem:** Initial schema used only `processing_time`, making PIT queries impossible.

**Solution:** 
```sql
-- WRONG: Only has processing time
CREATE TABLE features (
    account_id String,
    processing_time DateTime DEFAULT now()
)

-- CORRECT: Has both event and processing time
CREATE TABLE features (
    account_id String,
    event_time DateTime,          -- When business event occurred
    processing_time DateTime,     -- When pipeline received it
    version UInt64
) ENGINE = ReplacingMergeTree(version)
```

**Rule:** Always include explicit `event_time` column in ClickHouse tables for PIT correctness.

---

## Kafka Idempotent Producers

### Lesson: Enable Idempotence at Producer Level

**Problem:** Retry logic in application code wasn't sufficient for exactly-once semantics.

**Solution:**
```python
# Kafka producer config
producer_config = {
    'bootstrap.servers': 'kafka:9092',
    'enable.idempotence': True,  # Critical!
    'acks': 'all',
    'max.in.flight.requests.per.connection': 5,
    'retries': 5
}
```

**Rule:** Always enable `enable.idempotence=True` in Kafka producers for financial data.

---

## PII Tokenization

### Lesson: Tokenize Before Kafka, Not After

**Problem:** Raw PII was briefly visible in Kafka topics before tokenization job could process it.

**Solution:**
```python
# Tokenize IMMEDIATELY after CDC capture
def cdc_handler(event):
    # FIRST: Tokenize
    if 'ssn' in event:
        event['ssn_token'] = tokenizer.tokenize(event['ssn'])
        del event['ssn']
    
    # THEN: Send to Kafka
    kafka_producer.send('transactions.raw', event)
```

**Rule:** Tokenize PII at the earliest possible point - ideally in the CDC connector itself.

---

## Flink State Backend

### Lesson: RocksDB for Large State

**Problem:** Heap state backend caused OOM errors when tracking >1M accounts.

**Solution:**
```python
# Use RocksDB for large state
env.set_state_backend(RocksDBStateBackend(
    checkpoint_storage=FileSystemCheckpointStorage(
        "s3://feature-store-checkpoints/"
    )
))
```

**Rule:** Use RocksDB state backend when state size exceeds available heap memory.

---

## Late Arrival Handling

### Lesson: Side Output for Late Data

**Problem:** Late events were silently dropped, causing aggregation inaccuracies.

**Solution:**
```python
# Configure side output for late data
class LateDataOutputTag(OutputTag):
    pass

late_tag = LateDataOutputTag("late-data")

windowed_stream.side_output(late_tag).add_sink(
    KafkaSink("transactions.late")
)
```

**Rule:** Always configure side output for late data to enable monitoring and reprocessing.

---

## Checkpoint Recovery

### Lesson: Test Checkpoint Recovery Regularly

**Problem:** Checkpoint recovery hadn't been tested; discovered corruption during incident.

**Solution:**
```bash
# Weekly automated recovery test
./test_checkpoint_recovery.sh:
  1. Create savepoint
  2. Cancel job
  3. Restore from savepoint
  4. Compare state with replay
  5. Alert if mismatch
```

**Rule:** Automate checkpoint recovery testing as part of CI/CD pipeline.

---

## Schema Evolution

### Lesson: Backward Compatibility for CDC Schemas

**Problem:** Schema change in source DB broke CDC pipeline.

**Solution:**
```python
# Schema Registry with backward compatibility
schema_registry_client = CachedSchemaRegistryClient(
    url='http://schema-registry:8081',
    max_schemas_per_subject=100
)

# Register schema with BACKWARD compatibility
schema_registry_client.register(
    subject='transactions-value',
    schema=schema,
    compatibility='BACKWARD'
)
```

**Rule:** Always use Schema Registry with BACKWARD compatibility for CDC schemas.

---

## Data Lineage

### Lesson: Propagate Trace Context Through Pipeline

**Problem:** Couldn't trace feature back to source event during audit.

**Solution:**
```python
# Add trace_id to every event at ingestion
def ingest_event(event):
    event['trace_id'] = str(uuid.uuid4())
    event['ingestion_timestamp'] = datetime.utcnow().isoformat()
    kafka_producer.send(topic, event)

# Pass through Flink operators
class LineageTrackingProcessFunction(ProcessFunction):
    def process_element(self, element, ctx):
        # Preserve trace_id
        output = element.copy()
        output['flink_processing_time'] = datetime.utcnow().isoformat()
        yield output
```

**Rule:** Inject trace_id at pipeline entry and preserve through all transformations.

---

## Performance Optimization

### Lesson: Batch ClickHouse Writes

**Problem:** Individual row inserts caused high ClickHouse load.

**Solution:**
```python
# Batch writes in Flink sink
class ClickHouseSink:
    def __init__(self, batch_size=1000, flush_interval=5):
        self.buffer = []
        self.batch_size = batch_size
        self.flush_interval = flush_interval
    
    def invoke(self, value):
        self.buffer.append(value)
        if len(self.buffer) >= self.batch_size:
            self.flush()
```

**Rule:** Batch ClickHouse writes (1000+ rows) for high-throughput pipelines.

---

## Security

### Lesson: Validate All Incoming Data

**Problem:** Malformed events caused pipeline crashes.

**Solution:**
```python
# Strict validation at pipeline entry
def validate_event(event):
    required_fields = ['transaction_id', 'account_id', 'amount', 'event_time']
    for field in required_fields:
        if field not in event:
            raise ValidationError(f"Missing required field: {field}")
    
    # Validate types
    if not isinstance(event['amount'], (int, float)):
        raise ValidationError("amount must be numeric")
    
    # Validate timestamp
    if event['event_time'] < '2020-01-01':
        raise ValidationError("event_time too old")
    
    return True
```

**Rule:** Validate all incoming data at pipeline boundary - never trust external sources.

---

## Summary of Rules

1. **Watermarks:** Add 5-10 second buffer for clock skew in multi-source CDC
2. **ClickHouse:** Always include explicit `event_time` column for PIT queries
3. **Kafka:** Enable `enable.idempotence=True` for financial data
4. **PII:** Tokenize at earliest possible point (CDC connector level)
5. **Flink State:** Use RocksDB when state > available heap
6. **Late Data:** Configure side output for monitoring and reprocessing
7. **Checkpoints:** Test recovery weekly via automation
8. **Schema:** Use Schema Registry with BACKWARD compatibility
9. **Lineage:** Inject trace_id at entry, preserve through pipeline
10. **Performance:** Batch ClickHouse writes (1000+ rows)
11. **Security:** Validate all incoming data at pipeline boundary
