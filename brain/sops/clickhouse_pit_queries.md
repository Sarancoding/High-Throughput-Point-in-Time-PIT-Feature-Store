# ClickHouse PIT Queries SOP

## Overview

This SOP defines standard procedures for implementing Point-in-Time (PIT) queries in ClickHouse for the feature store.

## Table Engine Selection

### ReplacingMergeTree for Deduplication

Use when you need to deduplicate events based on a version or timestamp:

```sql
CREATE TABLE transactions_raw (
    transaction_id String,
    account_id String,
    amount Decimal(18, 2),
    event_time DateTime,
    processing_time DateTime DEFAULT now(),
    version UInt64 DEFAULT 1
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_time)
ORDER BY (account_id, event_time, transaction_id);
```

**Key Points**:
- `version` column determines which row is kept (higher wins)
- Deduplication happens during merges, not inserts
- Use `FINAL` modifier in queries to see deduplicated data

### AggregatingMergeTree for Pre-computed Features

Use for materialized aggregations:

```sql
CREATE TABLE account_features (
    account_id String,
    feature_date Date,
    hourly_volume AggregateFunction(sum, Decimal(18, 2)),
    transaction_count AggregateFunction(count, UInt64),
    avg_amount AggregateFunction(avg, Decimal(18, 2))
) ENGINE = AggregatingMergeTree()
PARTITION BY feature_date
ORDER BY (account_id, feature_date);
```

### CollapsingMergeTree for State Tracking

Use for tracking state changes with sign columns:

```sql
CREATE TABLE account_state (
    account_id String,
    event_time DateTime,
    balance Decimal(18, 2),
    sign Int8  -- 1 for insert, -1 for delete/update
) ENGINE = CollapsingMergeTree(sign)
ORDER BY (account_id, event_time);
```

## AS OF Query Patterns

### Basic AS OF Query

Retrieve state as of a specific timestamp:

```sql
SELECT 
    account_id,
    argMax(balance, event_time) AS balance_as_of
FROM account_state
WHERE event_time <= '2024-01-15 10:00:00'
GROUP BY account_id;
```

### AS OF with ReplacingMergeTree

```sql
SELECT 
    t.account_id,
    t.amount,
    t.event_time
FROM transactions_raw FINAL
WHERE t.account_id = 'A123'
  AND t.event_time <= '2024-01-15 10:00:00'
ORDER BY t.event_time DESC
LIMIT 1;
```

### Temporal Join (AS OF JOIN)

Join features with labels at correct point in time:

```sql
SELECT 
    f.account_id,
    f.feature_value,
    l.label_value
FROM features_table AS f
AS OF TIMESTAMP f.event_time
JOIN labels_table AS l
ON f.account_id = l.account_id
AND l.event_time <= f.event_time;
```

ClickHouse implementation using `argMax`:

```sql
SELECT 
    f.account_id,
    f.event_time AS feature_time,
    f.feature_value,
    argMax(l.label_value, l.event_time) AS label_at_feature_time
FROM features_table f
LEFT JOIN labels_table l ON f.account_id = l.account_id
WHERE l.event_time <= f.event_time
GROUP BY f.account_id, f.event_time, f.feature_value;
```

## Windowed Aggregations

### Tumbling Window (Fixed Size)

```sql
SELECT 
    account_id,
    toStartOfHour(event_time) AS window_start,
    sum(amount) AS hourly_volume,
    count() AS transaction_count
FROM transactions_raw
WHERE event_time >= '2024-01-15 00:00:00'
  AND event_time < '2024-01-16 00:00:00'
GROUP BY account_id, window_start;
```

### Sliding Window

```sql
SELECT 
    account_id,
    event_time,
    sum(amount) OVER (
        PARTITION BY account_id 
        ORDER BY event_time 
        ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
    ) AS rolling_60_tx_volume
FROM transactions_raw;
```

### Session Window (Gap-Based)

```sql
-- Using window functions to detect sessions
WITH ordered_tx AS (
    SELECT 
        account_id,
        event_time,
        amount,
        lag(event_time) OVER (PARTITION BY account_id ORDER BY event_time) AS prev_time
    FROM transactions_raw
),
session_boundaries AS (
    SELECT 
        *,
        if(dateDiff('second', prev_time, event_time) > 1800, 1, 0) AS is_new_session
    FROM ordered_tx
),
session_ids AS (
    SELECT 
        *,
        sum(is_new_session) OVER (PARTITION BY account_id ORDER BY event_time) AS session_id
    FROM session_boundaries
)
SELECT 
    account_id,
    session_id,
    min(event_time) AS session_start,
    max(event_time) AS session_end,
    sum(amount) AS session_volume
FROM session_ids
GROUP BY account_id, session_id;
```

## Performance Optimization

### Indexing Strategy

```sql
-- Add skip indices for common filters
ALTER TABLE transactions_raw 
ADD INDEX idx_account_id account_id TYPE minmax GRANULARITY 4;

ALTER TABLE transactions_raw
ADD INDEX idx_event_time event_time TYPE minmax GRANULARITY 4;
```

### Projection for Common Queries

```sql
-- Create projection for frequent query pattern
ALTER TABLE transactions_raw
ADD PROJECTION hourly_aggregates (
    SELECT 
        account_id,
        toStartOfHour(event_time) AS hour,
        sum(amount) AS total_amount,
        count() AS tx_count
    GROUP BY account_id, hour
    ORDER BY account_id, hour
);

-- Materialize existing data
ALTER TABLE transactions_raw MATERIALIZE PROJECTION hourly_aggregates;
```

### TTL for Data Retention

```sql
-- Auto-delete old raw data, keep aggregations
ALTER TABLE transactions_raw
MODIFY TTL event_time + INTERVAL 90 DAY;

-- Move old data to cold storage
ALTER TABLE transactions_raw
MODIFY TTL event_time + INTERVAL 7 DAY TO VOLUME 'cold',
         event_time + INTERVAL 90 DAY DROP;
```

## Query Examples by Use Case

### Feature Retrieval for ML Inference

```sql
-- Get latest features as of prediction time
SELECT 
    account_id,
    argMax(feature_name, feature_version) AS feature_name,
    argMax(feature_value, feature_version) AS feature_value
FROM feature_store
WHERE account_id IN ('A123', 'A456', 'A789')
  AND feature_time <= '2024-01-15 10:00:00'
GROUP BY account_id;
```

### Historical Backtesting

```sql
-- Replay features as they existed on historical date
SELECT 
    account_id,
    feature_date,
    feature_value
FROM feature_store FINAL
WHERE account_id = 'A123'
  AND feature_date BETWEEN '2024-01-01' AND '2024-01-31'
ORDER BY feature_date;
```

### Audit Trail Query

```sql
-- Get full history of feature changes
SELECT 
    account_id,
    feature_name,
    feature_value,
    version,
    updated_at
FROM feature_store
WHERE account_id = 'A123'
  AND feature_name = 'hourly_volume'
ORDER BY version;
```

## Common Pitfalls

### ❌ Missing FINAL Modifier

```sql
-- WRONG: May return duplicate/obsolete rows
SELECT * FROM transactions_raw WHERE account_id = 'A123';

-- CORRECT: Returns deduplicated rows
SELECT * FROM transactions_raw FINAL WHERE account_id = 'A123';
```

### ❌ Incorrect Timestamp Comparison

```sql
-- WRONG: Includes future data
SELECT * FROM features WHERE event_time = '2024-01-15 10:00:00';

-- CORRECT: Uses <= for PIT correctness
SELECT * FROM features WHERE event_time <= '2024-01-15 10:00:00';
```

### ❌ Not Handling Late Arrivals

```sql
-- WRONG: Assumes all data arrived on time
SELECT sum(amount) FROM transactions 
WHERE event_time >= '2024-01-15 10:00:00' 
  AND event_time < '2024-01-15 11:00:00';

-- CORRECT: Allow for late arrivals with buffer
SELECT sum(amount) FROM transactions 
WHERE event_time >= '2024-01-15 10:00:00' 
  AND event_time < '2024-01-15 11:00:00'
  AND processing_time <= '2024-01-15 11:15:00';  -- 15min latency buffer
```

## References

- `brain/strategy_docs/pit_semantics.md` - PIT theory
- `storage/clickhouse_schema.sql` - Schema definitions
- `storage/pit_query_engine.py` - Query builder implementation
