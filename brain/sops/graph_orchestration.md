# Data Lineage & Graph Orchestration SOP

## Overview

This SOP defines the data lineage tracking and orchestration graph for the PIT feature store pipeline.

## Data Lineage Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Source DB   │────▶│ CDC (Debez) │────▶│ Kafka Topic │────▶│ Flink Job   │────▶│ ClickHouse  │────▶│ ML Model    │
│ (PostgreSQL)│     │             │     │             │     │             │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │                   │                   │                   │
       ▼                   ▼                   ▼                   ▼                   ▼                   ▼
  Schema               Offset              Partition           Operator            Version            Prediction
  Version              Position            Key                 Checkpoint          Timestamp          ID
```

## Lineage Metadata

### Capture at Each Stage

| Stage | Metadata Captured |
|-------|-------------------|
| Source DB | Table schema, primary key, last modified |
| CDC | LSN/SCN, capture timestamp, operation type |
| Kafka | Topic, partition, offset, timestamp |
| Flink | Job ID, operator name, checkpoint ID, watermark |
| ClickHouse | Table version, part ID, mutation ID |
| Feature Access | Request ID, timestamp, user, purpose |

### Lineage Record Schema

```json
{
  "lineage_id": "lin_abc123",
  "trace_id": "trc_xyz789",
  "stages": [
    {
      "stage": "source",
      "system": "postgresql",
      "entity": "transactions_table",
      "schema_version": "v1.2",
      "timestamp": "2024-01-15T10:00:00Z"
    },
    {
      "stage": "cdc",
      "system": "debezium",
      "connector": "transactions_connector",
      "lsn": "0/1234567",
      "capture_timestamp": "2024-01-15T10:00:01Z"
    },
    {
      "stage": "kafka",
      "topic": "transactions.raw",
      "partition": 3,
      "offset": 987654,
      "timestamp": "2024-01-15T10:00:01Z"
    },
    {
      "stage": "flink",
      "job_id": "flink_job_001",
      "operator": "WindowAggregator",
      "checkpoint_id": 42,
      "watermark": "2024-01-15T09:55:00Z",
      "timestamp": "2024-01-15T10:00:02Z"
    },
    {
      "stage": "clickhouse",
      "table": "account_features",
      "part_id": "all_1_1_0",
      "version": 5,
      "timestamp": "2024-01-15T10:00:03Z"
    },
    {
      "stage": "feature_access",
      "request_id": "req_def456",
      "user": "ml_model_001",
      "purpose": "fraud_prediction",
      "timestamp": "2024-01-15T10:05:00Z"
    }
  ]
}
```

## Ingestion DAG

```python
# Async event-driven graph for CDC ingestion
INGESTION_DAG = {
    "nodes": [
        {
            "id": "cdc_source",
            "type": "source",
            "config": {
                "connector": "debezium-postgres",
                "database": "trading_db",
                "tables": ["transactions", "accounts"]
            }
        },
        {
            "id": "schema_validator",
            "type": "transform",
            "config": {
                "schema_registry": "http://schema-registry:8081",
                "compatibility": "BACKWARD"
            }
        },
        {
            "id": "idempotent_writer",
            "type": "sink",
            "config": {
                "kafka_topic": "transactions.raw",
                "idempotence": True,
                "acks": "all"
            }
        }
    ],
    "edges": [
        ("cdc_source", "schema_validator"),
        ("schema_validator", "idempotent_writer")
    ]
}
```

## Stream Processing Graph (Flink DAG)

```python
FLINK_DAG = {
    "source": {
        "type": "KafkaConsumer",
        "topics": ["transactions.raw"],
        "group_id": "pit-feature-group",
        "auto_offset_reset": "earliest"
    },
    "operators": [
        {
            "name": "WatermarkAssigner",
            "type": "assign_timestamps_and_watermarks",
            "config": {
                "timestamp_extractor": "event_time_field",
                "watermark_strategy": "bounded_out_of_orderness",
                "max_out_of_orderness_seconds": 300
            }
        },
        {
            "name": "Deduplicator",
            "type": "keyed_process_function",
            "config": {
                "key_selector": "transaction_id",
                "state_descriptor": "seen_transactions",
                "ttl_seconds": 86400
            }
        },
        {
            "name": "AccountKeyer",
            "type": "key_by",
            "config": {
                "key_field": "account_id"
            }
        },
        {
            "name": "WindowedAggregator",
            "type": "window_aggregate",
            "config": {
                "window_type": "tumbling_event_time_windows",
                "window_size_seconds": 3600,
                "allowed_lateness_seconds": 900,
                "aggregations": ["sum(amount)", "count(*)", "avg(amount)"]
            }
        },
        {
            "name": "LateDataHandler",
            "type": "side_output",
            "config": {
                "tag": "late_data",
                "sink": "kafka:transactions.late"
            }
        }
    ],
    "sink": {
        "type": "ClickHouseSink",
        "table": "account_features",
        "write_mode": "upsert",
        "batch_size": 1000,
        "flush_interval_seconds": 5
    }
}
```

## Storage Query Graph

```python
PIT_QUERY_GRAPH = {
    "input": {
        "account_ids": List[str],
        "as_of_timestamp": DateTime,
        "feature_names": List[str]
    },
    "steps": [
        {
            "step": 1,
            "operation": "AS_OF_FILTER",
            "description": "Filter features as of timestamp",
            "sql_template": "WHERE event_time <= %(as_of)s"
        },
        {
            "step": 2,
            "operation": "DEDUPLICATION",
            "description": "Get latest version per account/feature",
            "sql_template": "SELECT * FROM table FINAL"
        },
        {
            "step": 3,
            "operation": "PII_MASKING",
            "description": "Remove/tokenize any PII fields",
            "function": "pii_tokenizer.mask_features()"
        },
        {
            "step": 4,
            "operation": "VECTOR_ASSEMBLY",
            "description": "Assemble features into ML-ready vector",
            "output_format": "numpy_array"
        }
    ],
    "output": {
        "feature_vectors": np.ndarray,
        "metadata": {
            "as_of": DateTime,
            "accounts_processed": int,
            "features_returned": int,
            "query_latency_ms": float
        }
    }
}
```

## Lineage Tracking Implementation

### Trace Context Propagation

```python
import uuid
from contextvars import ContextVar

# Thread-local trace context
TRACE_CONTEXT: ContextVar[dict] = ContextVar('trace_context', default={})

def start_trace(source_event: dict) -> str:
    """Initialize trace context at pipeline entry"""
    trace_id = str(uuid.uuid4())
    lineage_record = {
        "trace_id": trace_id,
        "started_at": datetime.utcnow().isoformat(),
        "stages": []
    }
    TRACE_CONTEXT.set(lineage_record)
    return trace_id

def add_lineage_stage(stage_data: dict):
    """Add stage to current trace"""
    context = TRACE_CONTEXT.get()
    context["stages"].append(stage_data)
    TRACE_CONTEXT.set(context)

def complete_trace() -> dict:
    """Finalize and persist trace"""
    context = TRACE_CONTEXT.get()
    context["completed_at"] = datetime.utcnow().isoformat()
    
    # Persist to lineage store
    lineage_store.save(context)
    
    return context
```

### Audit Log Integration

```python
class AuditLogger:
    def log_feature_access(self, account_id: str, features: list, user: str):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": "feature_access",
            "account_id": hash_pii(account_id),
            "features_requested": features,
            "user": user,
            "trace_id": TRACE_CONTEXT.get().get("trace_id")
        }
        self.audit_log.write(log_entry)
```

## References

- `brain/strategy_docs/financial_compliance.md` - Compliance requirements
- `security/audit_logger.py` - Audit logging implementation
- `observability/grafana_dashboards.json` - Lineage visualization
