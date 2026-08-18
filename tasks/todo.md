# PIT Feature Store - Task List

## Phase 1: Company Brain (Knowledge Layer) ✅

- [x] Create `brain/strategy_docs/pit_semantics.md`
- [x] Create `brain/strategy_docs/idempotency_patterns.md`
- [x] Create `brain/strategy_docs/financial_compliance.md`
- [x] Create `brain/strategy_docs/pii_protection.md`
- [x] Create `brain/sops/flink_state_management.md`
- [x] Create `brain/sops/clickhouse_pit_queries.md`
- [x] Create `brain/sops/graph_orchestration.md`
- [x] Create `brain/sops/prompting_techniques.md`
- [x] Create `brain/examples/good_audit_report.md`
- [x] Create `brain/client_learnings.md`
- [x] Copy `AGENTS.md` to repo root
- [x] Create `agents/ingestion_agent.md`
- [x] Create `agents/stream_agent.md`
- [x] Create `agents/storage_agent.md`
- [x] Create `agents/security_agent.md`
- [x] Create `agents/observability_agent.md`
- [x] Create `agents/analyst_agent.md`

## Phase 2: Harness & Orchestrator ⏳

- [ ] Create `harness/CLAUDE.md`
- [ ] Create `harness/openclawed_loops.md`
- [ ] Create `harness/.env.example`
- [ ] Create `orchestrator/router.py` or Makefile
- [ ] Define token management and context propagation

## Phase 3: Execution & Build ⏳

### Ingestion (Subagent A)
- [ ] Create `infra/kafka_producer.py`
- [ ] Create `infra/debezium_config.json`
- [ ] Create `infra/schema_registry.py`

### Stream Processing (Subagent B)
- [ ] Create `stream/flink_job.py`
- [ ] Create `stream/polars_pipeline.py` (optional)

### Storage (Subagent C)
- [ ] Create `storage/clickhouse_schema.sql`
- [ ] Create `storage/pit_query_engine.py`

### Security (Subagent D)
- [ ] Create `security/pii_tokenizer.py`
- [ ] Create `security/audit_logger.py`
- [ ] Create `redteam/adversarial_data.json` (200+ payloads)
- [ ] Create `redteam/harness.py`
- [ ] Create `redteam/scorer.py`

### Observability (Subagent E)
- [ ] Create `observability/prometheus_metrics.py`
- [ ] Create `observability/grafana_dashboards.json`

### Analyst (Subagent F)
- [ ] Create `scripts/generate_pit_report.py`
- [ ] Create `scripts/generate_docs.py`

## Phase 4: Testing & Verification ⏳

- [ ] Create `tests/test_pit_correctness.py`
- [ ] Create `tests/test_idempotency.py`
- [ ] Create `tests/test_late_arrivals.py`
- [ ] Create `tests/test_security.py`
- [ ] Run all tests and verify 0 critical failures
- [ ] Generate `artifacts/pit_compliance_audit_report.md`

## Phase 5: Documentation & PDFs ⏳

- [ ] Create `docs/INSTALLATION.md`
- [ ] Create `docs/SETUP.md`
- [ ] Create `docs/SYSTEM_REQUIREMENTS.md`
- [ ] Create `docs/WORKFLOW.md`
- [ ] Generate `PIT_FeatureStore_Readme.pdf`
- [ ] Generate `PIT_FeatureStore_Installation_Guide.pdf`
- [ ] Generate `PIT_FeatureStore_Setup_Guide.pdf`
- [ ] Generate `PIT_FeatureStore_System_Requirements.pdf`
- [ ] Generate `PIT_FeatureStore_Workflow.pdf`
- [ ] Generate `PIT_FeatureStore_Testing_Report.pdf`
- [ ] Create `README.md` with links to all PDFs
- [ ] Create `GITMORE.md` with dev log

## Phase 6: GitHub Deployment ⏳

- [ ] Verify `.gitignore` excludes sensitive files
- [ ] Verify all 6 PDFs are in repo root
- [ ] Verify README links to all PDFs
- [ ] Run pre-push verification checklist
- [ ] Initialize git repo with main branch
- [ ] Commit all files
- [ ] Push to GitHub
- [ ] Verify landing page renders correctly
- [ ] Confirm all PDFs visible and downloadable

## Lessons Learned Log

See `tasks/lessons.md` for ongoing lessons.
