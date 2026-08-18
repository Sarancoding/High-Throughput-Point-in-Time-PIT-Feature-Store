# CLAUDE.md - AI Agent Instructions

## Project Context
This is a **High-Throughput Point-in-Time (PIT) Feature Store** for financial data. 
Security, Privacy, and Data Integrity are the top priorities.

## Key Constraints (from AGENTS.md)
1. **Do not preserve backward compatibility** - Remove obsolete paths
2. **Choose the simplest implementation** that works
3. **Keep components modular** with clearly separated concerns
4. **Prefer established, well-maintained libraries**
5. **Make architectural decisions for the long term** - No stopgaps
6. **Taste is the gate** - Judge output against examples in brain/

## Workflow
1. **Read brain/** first - All strategy docs and SOPs
2. **Fan out** - Launch subagents for parallel work
3. **Cross-verify** - Have agents test each other's work
4. **Merge** - Combine into clean, verified output
5. **Eval Loop** - Judge against brain/examples/good_audit_report.md

## Critical Rules
- **Zero future data leakage** allowed in PIT computations
- **100% PII masking** required
- **Exactly-once semantics** for CDC streams
- **All 6 PDFs must be generated** and visible on GitHub landing page

## Token Management
- Track `module_id`, `prompt_type`, `token_count`, `cost_usd` for all LLM calls
- Cap reflexion/debugging loops at 3 iterations
- Enforce per-request token budgets

## Data Lineage
Track: CDC Source → Kafka → Flink State → ClickHouse → Feature Vectors
Every transformation must be logged with timestamps and schema versions.
