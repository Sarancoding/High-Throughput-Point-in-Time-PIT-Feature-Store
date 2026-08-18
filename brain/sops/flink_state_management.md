# Flink State Management SOP

## Overview

This SOP defines standard procedures for managing Apache Flink state in the PIT feature store pipeline.

## Checkpointing Configuration

### Production Settings

```python
from pyflink.common import Configuration, CheckpointingMode

def configure_checkpointing(env):
    """Configure Flink checkpointing for exactly-once semantics"""
    
    # Enable checkpointing every 60 seconds
    env.enable_checkpointing(
        interval=60000,
        mode=CheckpointingMode.EXACTLY_ONCE,
        timeout=600000,      # 10 minute timeout
        min_pause_between_checkpoints=30000,  # 30s between checkpoints
        max_concurrent_checkpoints=1
    )
    
    # Configure restart strategy
    env.set_restart_strategy(
        fixed_delay_restart_strategy(
            attempts=3,
            delay=10000  # 10 seconds between restarts
        )
    )
    
    # Enable externalized checkpoints
    config = Configuration()
    config.set_string(
        "execution.checkpointing.externalized-checkpoint-retention",
        "RETAIN_ON_CANCELLATION"
    )
```

### State Backend Selection

| Use Case | Backend | Storage | Recovery Time |
|----------|---------|---------|---------------|
| Low latency (<100ms) | HashMapStateBackend | Heap + RocksDB | Fast |
| Large state (>10GB) | RocksDBStateBackend | Local SSD + S3 | Medium |
| High availability | RocksDBStateBackend | S3/GCS | Slow |

### Recommended: RocksDB with S3

```python
from pyflink.common import Configuration

def configure_rocksdb_backend():
    config = Configuration()
    
    # Set RocksDB state backend
    config.set_string("state.backend", "rocksdb")
    
    # Configure incremental checkpoints
    config.set_string("state.backend.rocksdb.incremental-checkpoint.enabled", "true")
    
    # Configure checkpoint storage (S3)
    config.set_string("state.checkpoints.dir", "s3://feature-store-checkpoints/")
    config.set_string("state.savepoints.dir", "s3://feature-store-savepoints/")
    
    # Configure RocksDB memory
    config.set_string("state.backend.rocksdb.memory.managed", "true")
    config.set_string("state.backend.rocksdb.memory.fixed-per-slot", "256mb")
```

## Savepoint Operations

### Creating a Savepoint

```bash
# Trigger savepoint via CLI
./bin/flink savepoint <job_id> s3://feature-store-savepoints/sp-20240115-100000/

# Via REST API
curl -X POST -H "Content-Type: application/json" \
  http://flink-jobmanager:8081/jobs/<job_id>/savepoints \
  -d '{"target-directory": "s3://feature-store-savepoints/", "cancel-job": false}'
```

### Recovering from Savepoint

```bash
# Submit job from savepoint
./bin/flink run \
  -s s3://feature-store-savepoints/sp-20240115-100000/ \
  ./pit-feature-job.jar
```

### Programmatic Savepoint

```python
from pyflink.datastream import StreamExecutionEnvironment

def trigger_savepoint(env, job_id, target_path):
    """Trigger savepoint programmatically"""
    env.stop_job_with_savepoint(job_id, target_path, terminate=False)
```

## State Schema Evolution

### Versioned State

```python
class FeatureStateV1:
    def __init__(self):
        self.total = 0.0
        self.count = 0

class FeatureStateV2:
    """Added min/max tracking"""
    def __init__(self):
        self.total = 0.0
        self.count = 0
        self.min = float('inf')
        self.max = float('-inf')
    
    @classmethod
    def from_v1(cls, v1_state):
        return cls(
            total=v1_state.total,
            count=v1_state.count,
            min=v1_state.total / v1_state.count if v1_state.count > 0 else 0,
            max=v1_state.total / v1_state.count if v1_state.count > 0 else 0
        )
```

### State Migration

```python
def migrate_state(old_state_data):
    """Migrate state from V1 to V2 schema"""
    new_state = {}
    for key, value in old_state_data.items():
        if isinstance(value, FeatureStateV1):
            new_state[key] = FeatureStateV2.from_v1(value)
        else:
            new_state[key] = value
    return new_state
```

## Monitoring State

### Key Metrics

```python
# Prometheus metrics to track
FLINK_STATE_METRICS = [
    "flink_taskmanager_job_task_operator_state_size",
    "flink_taskmanager_job_task_operator_checkpoint_alignment_time",
    "flink_jobmanager_job_last_checkpoint_duration",
    "flink_jobmanager_job_last_checkpoint_size",
    "flink_taskmanager_job_task_numLateRecordsDropped",
]
```

### Alerting Rules

```yaml
# Prometheus alerting rules
groups:
  - name: flink_state
    rules:
      - alert: CheckpointTooLarge
        expr: flink_jobmanager_job_last_checkpoint_size > 1073741824  # 1GB
        for: 5m
        annotations:
          summary: "Flink checkpoint size exceeds 1GB"
      
      - alert: CheckpointTooSlow
        expr: flink_jobmanager_job_last_checkpoint_duration > 300000  # 5min
        for: 5m
        annotations:
          summary: "Flink checkpoint taking too long"
      
      - alert: LateRecordsDropped
        expr: rate(flink_taskmanager_job_task_numLateRecordsDropped[5m]) > 0
        annotations:
          summary: "Late records being dropped - check watermark settings"
```

## Troubleshooting

### Issue: Checkpoint Failures

**Symptoms**: Jobs failing, checkpoints timing out

**Diagnosis**:
```bash
# Check checkpoint history
curl http://flink-jobmanager:8081/jobs/<job_id>/checkpoints

# Check state backend logs
kubectl logs <taskmanager-pod> | grep -i checkpoint
```

**Resolution**:
1. Increase checkpoint timeout
2. Reduce state size (TTL, aggregation window)
3. Switch to incremental checkpoints
4. Scale up TaskManagers

### Issue: State Corruption

**Symptoms**: Incorrect aggregations after recovery

**Diagnosis**:
```bash
# Compare state before/after recovery
flink list -s <savepoint_path>
```

**Resolution**:
1. Restore from last known good savepoint
2. Replay events from Kafka offset
3. Verify deduplication logic

### Issue: Memory Pressure

**Symptoms**: OOM errors, slow checkpoints

**Resolution**:
1. Enable RocksDB (off-heap state)
2. Configure state TTL
3. Reduce parallelism or increase slots
4. Tune RocksDB memory settings

## References

- `brain/strategy_docs/idempotency_patterns.md` - Exactly-once semantics
- `observability/prometheus_metrics.py` - Metric definitions
- `tests/test_idempotency.py` - State recovery tests
