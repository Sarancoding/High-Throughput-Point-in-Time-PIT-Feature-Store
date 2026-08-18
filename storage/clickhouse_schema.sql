-- ClickHouse Schema for PIT Feature Store
-- Implements ReplacingMergeTree for upserts and AggregatingMergeTree for pre-aggregated features

-- ============================================
-- Raw Events Table (ReplacingMergeTree)
-- ============================================
-- Stores all incoming CDC events with deduplication
CREATE TABLE IF NOT EXISTS pit_features.raw_events
(
    event_id UUID DEFAULT generateUUIDv4(),
    transaction_id String,
    account_id String,
    event_time DateTime64(6),
    event_type LowCardinality(String),
    processing_time DateTime64(6) DEFAULT now64(6),
    watermark_time DateTime64(6),
    amount Float64,
    currency FixedString(3),
    merchant_id Nullable(String),
    category Nullable(String),
    is_late UInt8 DEFAULT 0,
    is_retraction UInt8 DEFAULT 0,
    _version UInt64 DEFAULT now64()
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (account_id, event_time, transaction_id)
SETTINGS 
    index_granularity = 8192,
    allow_nullable_key = 1;

-- ============================================
-- Account Features Table (AggregatingMergeTree)
-- ============================================
-- Pre-aggregated features for fast PIT queries
CREATE TABLE IF NOT EXISTS pit_features.account_features
(
    account_id String,
    feature_timestamp DateTime64(6),
    window_1h_start DateTime64(6),
    window_24h_start DateTime64(6),
    
    -- 1-hour window aggregations
    transaction_count_1h AggregateFunction(count, UInt64),
    total_amount_1h AggregateFunction(sum, Float64),
    avg_amount_1h AggregateFunction(avg, Float64),
    
    -- 24-hour window aggregations
    transaction_count_24h AggregateFunction(count, UInt64),
    total_amount_24h AggregateFunction(sum, Float64),
    avg_amount_24h AggregateFunction(avg, Float64),
    
    -- Additional features
    unique_merchants_24h AggregateFunction(uniq, String),
    max_amount_24h AggregateFunction(max, Float64),
    min_amount_24h AggregateFunction(min, Float64),
    
    -- Risk metrics
    risk_score Float32,
    
    -- Metadata
    last_updated DateTime64(6) DEFAULT now64(6),
    _version UInt64 DEFAULT now64()
)
ENGINE = AggregatingMergeTree(_version)
PARTITION BY toYYYYMM(feature_timestamp)
ORDER BY (account_id, feature_timestamp)
SETTINGS 
    index_granularity = 8192;

-- ============================================
-- Feature Snapshots Table (for PIT AS OF queries)
-- ============================================
-- Stores point-in-time snapshots of feature vectors
CREATE TABLE IF NOT EXISTS pit_features.feature_snapshots
(
    snapshot_id UUID DEFAULT generateUUIDv4(),
    account_id String,
    snapshot_time DateTime64(6),
    
    -- Feature values (materialized)
    transaction_count_1h UInt32,
    transaction_count_24h UInt32,
    total_amount_1h Float64,
    total_amount_24h Float64,
    avg_amount_1h Float64,
    avg_amount_24h Float64,
    unique_merchants_24h UInt32,
    max_amount_24h Float64,
    min_amount_24h Float64,
    risk_score Float32,
    
    -- Lineage tracking
    source_event_count UInt32,
    computation_version String,
    
    created_at DateTime64(6) DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMMDD(snapshot_time)
ORDER BY (account_id, snapshot_time)
SETTINGS 
    index_granularity = 4096;

-- ============================================
-- Audit Log Table
-- ============================================
-- Tracks all feature computations and accesses
CREATE TABLE IF NOT EXISTS pit_features.audit_log
(
    log_id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime64(6) DEFAULT now64(6),
    operation LowCardinality(String),  -- INSERT, UPDATE, QUERY, EXPORT
    account_id Nullable(String),
    feature_timestamp Nullable(DateTime64(6)),
    user_id Nullable(String),
    query_id Nullable(String),
    latency_ms UInt32,
    rows_processed UInt32,
    metadata String  -- JSON metadata
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, log_id)
SETTINGS 
    index_granularity = 8192;

-- ============================================
-- PII Tokenization Table
-- ============================================
-- Maps tokens to original PII values (encrypted at rest)
CREATE TABLE IF NOT EXISTS pit_features.pii_tokens
(
    token_id String,
    original_value String,  -- Encrypted
    pii_type LowCardinality(String),  -- SSN, ACCOUNT_NUMBER, etc.
    created_at DateTime64(6) DEFAULT now64(6),
    expires_at Nullable(DateTime64(6))
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (token_id)
TTL expires_at
SETTINGS 
    index_granularity = 8192;

-- ============================================
-- Watermark State Table
-- ============================================
-- Tracks watermarks per partition for late event handling
CREATE TABLE IF NOT EXISTS pit_features.watermark_state
(
    partition_key String,
    watermark_time DateTime64(6),
    last_event_time DateTime64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (partition_key)
SETTINGS 
    index_granularity = 1024;

-- ============================================
-- Materialized View: Compute 1-hour features
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS pit_features.mv_features_1h
TO pit_features.account_features
AS SELECT
    account_id,
    toStartOfHour(event_time) AS feature_timestamp,
    toStartOfHour(event_time) AS window_1h_start,
    toStartOfDay(event_time) AS window_24h_start,
    
    countState(toUInt64(1)) AS transaction_count_1h,
    sumState(amount) AS total_amount_1h,
    avgState(amount) AS avg_amount_1h,
    
    countState(toUInt64(1)) AS transaction_count_24h,
    sumState(amount) AS total_amount_24h,
    avgState(amount) AS avg_amount_24h,
    
    uniqState(merchant_id) AS unique_merchants_24h,
    maxState(amount) AS max_amount_24h,
    minState(amount) AS min_amount_24h,
    
    0.0 AS risk_score,
    now64(6) AS last_updated,
    now64() AS _version
FROM pit_features.raw_events
GROUP BY account_id, toStartOfHour(event_time), toStartOfDay(event_time);

-- ============================================
-- Sample PIT AS OF Query
-- ============================================
-- This query retrieves features as of a specific point in time
-- without any future data leakage

-- Example: Get features for account as of 2024-01-15 14:30:00
-- SELECT 
--     account_id,
--     argMax(
--         (transaction_count_24h, total_amount_24h, risk_score),
--         snapshot_time
--     ) AS latest_features
-- FROM pit_features.feature_snapshots
-- WHERE account_id = 'ACC_001'
--   AND snapshot_time <= '2024-01-15 14:30:00'
-- GROUP BY account_id;

-- ============================================
-- Indexes for Performance
-- ============================================
CREATE INDEX IF NOT EXISTS idx_account_id 
ON pit_features.raw_events (account_id) TYPE bloom_filter GRANULARITY 4;

CREATE INDEX IF NOT EXISTS idx_event_time 
ON pit_features.raw_events (event_time) TYPE minmax GRANULARITY 4;

CREATE INDEX IF NOT EXISTS idx_transaction_id 
ON pit_features.raw_events (transaction_id) TYPE bloom_filter GRANULARITY 4;
