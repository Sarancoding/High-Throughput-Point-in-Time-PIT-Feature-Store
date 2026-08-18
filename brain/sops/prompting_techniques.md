# Prompting Techniques Decision Tree

## Overview

When using LLMs to assist with SQL/Flink job generation or debugging lineage, select the appropriate prompting technique based on task complexity.

## Decision Tree

```
Task Complexity Assessment
         │
         ▼
┌─────────────────────┐
│  Is it a simple     │
│  lookup/template?   │
└──────────┬──────────┘
           │
     ┌─────┴─────┐
     │    YES    │
     │           │
     ▼           ▼
┌─────────┐  ┌──────────────────┐
│SINGLE-  │  │  Does it require │
│PASS     │  │  multi-step      │
│PROMPT   │  │  reasoning?      │
└─────────┘  └────────┬─────────┘
                      │
                ┌─────┴─────┐
                │    YES    │
                │           │
                ▼           ▼
         ┌───────────┐  ┌──────────────────┐
         │TREE OF    │  │  Is there a      │
         │THOUGHT    │  │  single correct  │
         │(ToT)      │  │  answer?         │
         └───────────┘  └────────┬─────────┘
                                 │
                           ┌─────┴─────┐
                           │    YES    │
                           │           │
                           ▼           ▼
                    ┌───────────┐  ┌──────────────────┐
                    │SELF-      │  │  SINGLE-PASS     │
                    │CONSISTENCY│  │  PROMPT          │
                    └───────────┘  └──────────────────┘
```

## Technique Selection

### Single-Pass Prompt

**Use when:**
- Simple template generation
- Straightforward SQL queries
- Basic configuration tasks
- Documentation generation

**Example: Generate ClickHouse Schema**

```
Generate a ClickHouse CREATE TABLE statement for storing 
transaction events with the following fields:
- transaction_id (String, primary key)
- account_id (String)
- amount (Decimal 18,2)
- event_time (DateTime)

Use ReplacingMergeTree engine with versioning.
```

### Self-Consistency

**Use when:**
- There's a single correct answer
- Validation is possible
- Examples include:
  - SQL query optimization
  - Bug fix suggestions
  - Configuration validation

**Process:**
1. Ask the same question 3-5 times with slight variations
2. Compare outputs for consistency
3. Select the most common/correct answer

**Example: Debug Flink Watermark Issue**

```
[Prompt 1]: Why might my Flink watermark be causing late records?
[Prompt 2]: What causes numLateRecordsDropped to increase in Flink?
[Prompt 3]: How do I fix watermark issues in event-time processing?

Compare answers and extract common solutions.
```

### Tree of Thought (ToT)

**Use when:**
- Complex multi-step reasoning required
- Multiple valid approaches exist
- Architectural decisions needed
- Debugging complex pipeline issues

**Process:**
1. Break problem into branches
2. Explore each branch independently
3. Evaluate tradeoffs
4. Synthesize best solution

**Example: Design PIT Feature Store Architecture**

```
Branch 1: Storage Options
  - ClickHouse vs. Druid vs. Pinot
  - Evaluate for PIT queries
  
Branch 2: Stream Processing
  - Flink vs. Spark Streaming vs. Kafka Streams
  - Evaluate for exactly-once semantics
  
Branch 3: State Management
  - RocksDB vs. Heap vs. External
  - Evaluate for recovery time

Synthesize: Best combination for financial PIT feature store
```

## Implementation Guidelines

### Token Budget Enforcement

```python
class PromptBudget:
    MAX_TOKENS_PER_REQUEST = 4096
    MAX_REFLEXION_ITERATIONS = 3
    
    def __init__(self):
        self.tokens_used = 0
        self.iterations = 0
    
    def can_proceed(self, estimated_tokens: int) -> bool:
        if self.iterations >= self.MAX_REFLEXION_ITERATIONS:
            return False
        if self.tokens_used + estimated_tokens > self.MAX_TOKENS_PER_REQUEST:
            return False
        return True
    
    def record_usage(self, tokens: int):
        self.tokens_used += tokens
        self.iterations += 1
```

### Context Isolation

```python
# Separate contexts for different data types
CDC_CONTEXT = """
You are generating code for CDC event processing.
Focus on: schema evolution, idempotency, offset tracking.
"""

PII_CONTEXT = """
You are generating code for PII handling.
Focus on: encryption, tokenization, access control, audit trails.
NEVER output raw PII values.
"""

FEATURE_CONTEXT = """
You are generating code for feature computation.
Focus on: PIT correctness, aggregation accuracy, latency.
"""
```

### Trace Metadata Emission

```python
def emit_llm_trace(module_id: str, prompt_type: str, response: dict):
    """Log LLM usage for cost attribution"""
    trace = {
        "timestamp": datetime.utcnow().isoformat(),
        "module_id": module_id,
        "prompt_type": prompt_type,
        "token_count": response["usage"]["total_tokens"],
        "cost_usd": calculate_cost(response["usage"]),
        "model": response["model"]
    }
    telemetry.log("llm_usage", trace)
```

## Examples by Task Type

### SQL Generation → Single-Pass

```
Prompt: Write a ClickHouse query to get hourly transaction volume 
per account as of timestamp T.

Expected Output: SELECT statement with argMax and GROUP BY
```

### Pipeline Debugging → Self-Consistency

```
Prompt Variation 1: Why is my Flink checkpoint failing?
Prompt Variation 2: What causes checkpoint timeouts in Flink?
Prompt Variation 3: How to optimize Flink checkpoint performance?

Synthesize common themes across responses.
```

### Architecture Design → Tree of Thought

```
Root: Design high-throughput PIT feature store

Branch 1 (Ingestion): Kafka vs. Pulsar vs. Kinesis
Branch 2 (Processing): Flink vs. Spark vs. Storm
Branch 3 (Storage): ClickHouse vs. Druid vs. TimescaleDB
Branch 4 (Serving): REST vs. gRPC vs. Direct Query

Evaluate each branch, then synthesize optimal architecture.
```

## References

- `brain/strategy_docs/pit_semantics.md` - Domain knowledge for prompts
- `orchestrator/router.py` - Routes tasks to appropriate agents
- `scripts/generate_docs.py` - Uses single-pass for doc generation
