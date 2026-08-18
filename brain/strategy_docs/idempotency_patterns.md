# Idempotency Patterns for CDC Streams

## Definition

**Idempotency** means that processing the same event multiple times produces the same result as processing it exactly once. In financial data pipelines, this is non-negotiable.

## Why Idempotency Matters

CDC (Change Data Capture) streams can deliver duplicate events due to:
- Kafka producer retries
- Consumer rebalancing
- Network timeouts
- Checkpoint recovery

Without idempotency, duplicate events corrupt aggregations and create incorrect feature values.

## Deduplication Keys

Every event must have a unique identifier for deduplication:

```python
# Composite deduplication key
dedup_key = f"{source_system}:{table_name}:{primary_key}:{event_time}:{sequence_number}"
```

### Key Components

1. **Source System**: Identifies origin (e.g., `trading_db`, `ledger_db`)
2. **Table Name**: Source table in the database
3. **Primary Key**: Business-level unique ID (e.g., `transaction_id`)
4. **Event Time**: Timestamp of the change
5. **Sequence Number**: CDC log sequence number (LSN)

## Upsert Logic

Use upsert (update-or-insert) semantics instead of append:

```sql
-- ClickHouse ReplacingMergeTree handles this automatically
-- Based on version column or latest timestamp

INSERT INTO feature_state (account_id, feature_name, value, version, updated_at)
VALUES ('A123', 'hourly_volume', 10000, 1, '2024-01-15 10:05:00')
ON CONFLICT (account_id, feature_name) 
DO UPDATE SET 
    value = EXCLUDED.value,
    version = EXCLUDED.version,
    updated_at = EXCLUDED.updated_at
WHERE EXCLUDED.version > feature_state.version;
```

## Exactly-Once Semantics in Flink

Apache Flink provides exactly-once guarantees through:

### 1. Checkpointing

```python
# Flink checkpoint configuration
env.enable_checkpointing(
    interval=60000,  # 1 minute
    mode=CheckpointingMode.EXACTLY_ONCE,
    timeout=600000,  # 10 minutes
    min_pause_between_checkpoints=30000,
    max_concurrent_checkpoints=1
)
```

### 2. Two-Phase Commit Sink

For transactional writes to external systems:

```python
# Two-phase commit sink for ClickHouse
sink = TwoPhaseCommitSink(
    transaction_manager=ClickHouseTxManager(),
    serializer=FeatureSerializer()
)
```

### 3. Idempotent Sink

If the sink supports idempotent writes (e.g., upsert by key):

```python
# ClickHouse ReplacingMergeTree with versioning
class IdempotentClickHouseSink:
    def write(self, record):
        # Uses REPLACE_BY_KEY or upsert logic
        pass
```

## Aggregation State Recovery

When recovering from a checkpoint, aggregation state must be reconstructed correctly:

### Pattern 1: Event Sourcing

Store all raw events and recompute aggregations on recovery:

```python
class AggregatorState:
    def __init__(self):
        self.events = []  # All events in window
    
    def add_event(self, event):
        self.events.append(event)
    
    def compute_aggregation(self):
        return sum(e.amount for e in self.events)
```

### Pattern 2: Incremental Updates with Retractions

Maintain running totals but emit retractions when late events arrive:

```python
class IncrementalAggregator:
    def __init__(self):
        self.total = 0
        self.seen_keys = set()
    
    def process(self, event):
        if event.id in self.seen_keys:
            return  # Duplicate, ignore
        self.seen_keys.add(event.id)
        self.total += event.amount
        return self.total
```

## Testing Idempotency

### Test Case 1: Duplicate Injection

```python
def test_duplicate_events():
    events = [
        {"id": "tx1", "amount": 100},
        {"id": "tx1", "amount": 100},  # Duplicate
        {"id": "tx2", "amount": 200},
    ]
    result = pipeline.process(events)
    assert result.total == 300  # Not 400
```

### Test Case 2: Out-of-Order Duplicates

```python
def test_out_of_order_duplicates():
    events = [
        {"id": "tx1", "amount": 100, "event_time": "10:00"},
        {"id": "tx2", "amount": 200, "event_time": "10:05"},
        {"id": "tx1", "amount": 100, "event_time": "10:00"},  # Late duplicate
    ]
    result = pipeline.process(events)
    assert result.total == 300
```

### Test Case 3: Checkpoint Recovery

```python
def test_checkpoint_recovery():
    # Process first half
    pipeline.process(events[:5])
    checkpoint = pipeline.save_checkpoint()
    
    # Recover and process second half
    pipeline.restore_checkpoint(checkpoint)
    pipeline.process(events[5:])
    
    # Compare with full replay
    full_result = pipeline.process_all(events)
    recovered_result = pipeline.get_state()
    assert full_result == recovered_result
```

## References

- `brain/strategy_docs/pit_semantics.md` - PIT correctness foundation
- `brain/sops/flink_state_management.md` - Flink state handling
- `brain/examples/good_audit_report.md` - Idempotency proof examples
