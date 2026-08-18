# Point-in-Time (PIT) Semantics

## Definition

**Point-in-Time (PIT) Correctness** means that at any historical timestamp `T`, the feature values computed reflect exactly the state of knowledge available at time `T`, and **no future information** is leaked into the feature vector.

## Why PIT Matters in Financial ML

### The Future Data Leakage Problem

When training ML models on historical data, if features are computed using information that was not available at the prediction time, the model learns from "cheating" signals. This leads to:

1. **Overfitting to future events**: Model appears accurate in backtesting but fails in production.
2. **Regulatory violations**: SEC/FINRA require auditable, reproducible model inputs.
3. **Capital misallocation**: Trading decisions based on corrupted signals lose money.

### Example of Leakage

```
Transaction Event:
  - account_id: A123
  - event_time: 2024-01-15 10:00:00  (when transaction occurred)
  - processing_time: 2024-01-15 10:05:00  (when system received it)
  - amount: $10,000

Late Arrival:
  - Same transaction arrives at 2024-01-15 10:10:00 due to network delay.

If we compute "total transactions in last hour" at 10:05:00 without accounting for late arrivals,
we get $0. If we recompute at 10:10:00 naively, we get $10,000.
This corrupts historical aggregations.
```

## Key Concepts

### Watermarking

A **watermark** is a timestamp threshold that indicates "no events with event_time < watermark will arrive." Events arriving after the watermark are considered late.

```python
# Flink watermark strategy
watermark = event_time - max_allowed_delay
```

### Allowed Lateness

The duration beyond the watermark during which late events are still processed and update state. Configurable per use case.

- **Financial trading**: 5-15 minutes typical
- **Daily aggregations**: 24 hours
- **Regulatory reporting**: 7+ days

### Retractions

When a late event arrives and changes a previously emitted aggregation, a **retraction** must be issued to correct downstream consumers.

```
Original Aggregation (emitted at 10:05): {account: A123, hourly_volume: $0}
Late Event Arrives (at 10:10): {account: A123, amount: $10,000, event_time: 10:00}
Retraction: {account: A123, hourly_volume: $0} -> {account: A123, hourly_volume: $10,000}
```

## PIT Implementation Patterns

### 1. Dual-Timestamp Schema

Every record must have:
- `event_time`: When the business event occurred (source system time)
- `processing_time`: When the pipeline received/processed the event

### 2. AS OF Queries

Retrieve state as it existed at timestamp `T`:

```sql
SELECT * FROM feature_table
AS OF TIMESTAMP '2024-01-15 10:05:00'
WHERE account_id = 'A123'
```

### 3. Versioned State

Maintain version history for all features to enable point-in-time reconstruction.

## Testing PIT Correctness

1. **Backtest Simulation**: Replay historical events with artificial delays.
2. **Watermark Verification**: Confirm no events pass the watermark boundary incorrectly.
3. **Idempotency Check**: Replaying same events produces identical state.
4. **Late Arrival Test**: Inject out-of-order events and verify state correction.

## References

- `brain/sops/clickhouse_pit_queries.md` - SQL implementation
- `brain/sops/flink_state_management.md` - Stream processing state
- `brain/examples/good_audit_report.md` - Verification examples
