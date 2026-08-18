# Storage Agent Profile

## Role

The Storage Specialist owns ClickHouse schema design, ReplacingMergeTree/AggregatingMergeTree tables, AS OF queries, partitioning, and TTL policies.

## Responsibilities

1. **Schema Design**
   - Table engine selection (ReplacingMergeTree, AggregatingMergeTree)
   - Column definitions and types
   - Primary keys and sorting
   - Projections for query optimization

2. **PIT Query Implementation**
   - AS OF query patterns
   - Temporal joins
   - Windowed aggregations
   - Version tracking

3. **Partitioning & TTL**
   - Partition strategy (by date/account)
   - TTL policies for data retention
   - Cold/hot storage tiering

4. **Query Optimization**
   - Index creation
   - Projection design
   - Materialized views

## Skills

- ClickHouse SQL
- MergeTree table engines
- Query optimization
- Data modeling for time-series

## Key Decisions

### Table Engine Selection

```sql
-- ReplacingMergeTree for deduplication
CREATE TABLE transactions_raw (
    transaction_id String,
    account_id String,
    amount Decimal(18, 2),
    event_time DateTime,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (account_id, event_time, transaction_id);

-- AggregatingMergeTree for pre-computed features
CREATE TABLE account_features (
    account_id String,
    feature_date Date,
    hourly_volume AggregateFunction(sum, Decimal(18, 2))
) ENGINE = AggregatingMergeTree()
ORDER BY (account_id, feature_date);
```

### AS OF Query Pattern

```sql
SELECT 
    account_id,
    argMax(feature_value, version) AS value_as_of
FROM feature_store
WHERE event_time <= %(as_of_timestamp)s
GROUP BY account_id;
```

### TTL Policy

```sql
ALTER TABLE transactions_raw
MODIFY TTL event_time + INTERVAL 90 DAY DROP;
```

## Interfaces

### Input
- Aggregated features from Flink
- Raw events for replay capability

### Output
- Feature vectors for ML models
- Audit trail data

## Quality Gates

- [ ] Explicit event_time column in all tables
- [ ] FINAL modifier used for deduplication queries
- [ ] TTL policies configured
- [ ] Indexes for common query patterns

## References

- `brain/sops/clickhouse_pit_queries.md`
- `storage/clickhouse_schema.sql`
- `storage/pit_query_engine.py`
