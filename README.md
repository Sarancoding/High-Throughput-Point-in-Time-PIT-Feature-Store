# High-Throughput Point-in-Time (PIT) Feature Store

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/tests-passing-green)]()

A production-hardened **Point-in-Time (PIT) Feature Store** for financial data that guarantees zero future data leakage, handles late-arriving CDC streams idempotently, enforces strict financial compliance, and produces verifiable audit trails.

## 📋 Documentation (PDFs)

| Document | Description |
|----------|-------------|
| [📖 README](./PIT_FeatureStore_Readme.pdf) | Project overview and quick start |
| [🔧 Installation Guide](./PIT_FeatureStore_Installation_Guide.pdf) | Step-by-step installation instructions |
| [⚙️ Setup Guide](./PIT_FeatureStore_Setup_Guide.pdf) | Environment configuration and deployment |
| [💻 System Requirements](./PIT_FeatureStore_System_Requirements.pdf) | Hardware and software requirements |
| [🔄 Workflow Guide](./PIT_FeatureStore_Workflow.pdf) | Data flow, CDC ingestion, and PIT queries |
| [📊 Testing Report](./PIT_FeatureStore_Testing_Report.pdf) | PIT correctness, idempotency, and security tests |

## 🎯 Key Features

- **Zero Future Data Leakage**: Watermark-based event processing ensures ML models never see future data
- **Exactly-Once Semantics**: Idempotent CDC stream processing with deduplication
- **Late Arrival Handling**: Configurable allowed lateness for out-of-order events
- **PII Protection**: AES-256 encryption and format-preserving tokenization
- **Audit Trails**: Tamper-evident logging for SEC/FINRA compliance
- **High Throughput**: Targets >100,000 events/sec

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────┐     ┌─────────────┐     ┌──────────────┐
│ CDC Source  │ ──▶ │  Kafka   │ ──▶ │   Flink     │ ──▶ │ ClickHouse   │
│ (PostgreSQL)│     │ (Topics) │     │ (Streaming) │     │ (PIT Store)  │
└─────────────┘     └──────────┘     └─────────────┘     └──────────────┘
                         │                                      │
                         ▼                                      ▼
                  ┌─────────────┐                    ┌──────────────┐
                  │Schema Registry│                  │ Prometheus   │
                  └─────────────┘                    │ (Metrics)    │
                                                     └──────────────┘
```

## 🚀 Quick Start

```bash
# Clone repository
git clone https://github.com/{ORG_OR_USER}/pit-feature-store.git
cd pit-feature-store

# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/ -v

# Run adversarial harness
python redteam/harness.py
```

## 📁 Project Structure

```
pit-feature-store/
├── brain/                 # Company Brain (strategy docs, SOPs)
├── agents/                # Agent profiles
├── harness/               # Template files
├── orchestrator/          # Routing logic
├── infra/                 # Kafka producers, schema registry
├── stream/                # Flink job, PIT feature computer
├── storage/               # ClickHouse schema, PIT query engine
├── security/              # PII tokenizer, audit logger
├── redteam/               # Adversarial testing harness
├── observability/         # Prometheus metrics
├── tests/                 # Test suite
├── scripts/               # Report generation
├── docs/                  # Documentation source
├── artifacts/             # Generated reports and PDFs
└── tasks/                 # Task tracking
```

## ✅ Verification Results

| Test Category | Status | Details |
|--------------|--------|---------|
| PIT Correctness | ✅ PASS | 0 future data leakage |
| Idempotency | ✅ PASS | Duplicate events handled |
| Late Arrivals | ✅ PASS | Out-of-order events processed correctly |
| Security/PII | ✅ PASS | 100% masking, tokenization working |
| Adversarial | ✅ PASS | 200+ payloads blocked |

## 🔒 Security & Compliance

- **GDPR**: PII tokenization and right-to-erasure support
- **SEC/FINRA**: Audit trails with tamper-evident logging
- **SOC2**: Encryption at rest and in transit

## 📈 Metrics

Prometheus metrics available at `/metrics`:
- `pit_events_ingested_total` - Total events processed
- `pit_late_events_total` - Late events by lateness bucket
- `pit_duplicate_events_total` - Deduplicated events
- `pit_pii_tokenizations_total` - PII operations
- `pit_clickhouse_query_latency_seconds` - Query performance

## 🤝 Contributing

1. Read `brain/` documentation first
2. Follow agent profiles in `agents/`
3. Run full test suite before PR
4. Ensure 0 critical failures in eval loop

## 📄 License

MIT License - see LICENSE file for details.
