# Lessons Learned Log

## Session Start: Initial Build

### Architecture Decisions

1. **Company Brain First**: Populating `brain/` with strategy docs and SOPs before coding ensures all agents have shared context. This prevents inconsistent implementations.

2. **Modular Agent Design**: Each agent owns a vertical slice (ingestion, stream, storage, security, observability, analyst). Clear separation of concerns enables parallel development.

3. **Simplest Implementation**: Following AGENTS.md principle #2 - choosing simplest implementation that satisfies requirements. No over-engineering.

### Key Rules Established

From `brain/client_learnings.md`:
- Always add 5-10 second buffer to watermarks for clock skew
- Always include explicit `event_time` column in ClickHouse for PIT queries
- Always enable `enable.idempotence=True` in Kafka producers
- Tokenize PII at earliest possible point (CDC connector level)
- Use RocksDB state backend when state > available heap
- Configure side output for late data monitoring
- Test checkpoint recovery weekly via automation
- Use Schema Registry with BACKWARD compatibility
- Inject trace_id at pipeline entry, preserve through transformations
- Batch ClickHouse writes (1000+ rows) for performance
- Validate all incoming data at pipeline boundary

### Open Questions

- Will use Python-based Flink (PyFlink) vs Polars based on team expertise
- ClickHouse cloud vs self-hosted decision pending infrastructure review
- PDF generation library selection: weasyprint vs reportlab

---

*This file will be updated throughout the build process.*
