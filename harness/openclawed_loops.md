# OpenClawed Loops

## CI/CD Execution Loop

```bash
# Local development loop
make setup-infra      # Start Kafka, Flink, ClickHouse, Prometheus
make build-stream-job # Compile and deploy Flink job
make apply-security   # Apply PII tokenization and audit logging
make instrument-metrics # Set up Prometheus/Grafana
make run-pit-tests    # Run PIT correctness tests
make generate-audit   # Generate compliance report
make push-to-github   # Deploy to GitHub
```

## Eval Loop

1. Run tests → Check results against `brain/examples/good_audit_report.md`
2. If ANY failure (leakage, PII exposure, non-idempotent):
   - Log to `tasks/lessons.md`
   - Re-route to appropriate agent
   - Re-run tests
3. Only proceed when 0 critical failures

## Data Integrity Gates

- **PIT Correctness**: 0 future data leakage allowed
- **Idempotency**: Duplicate events must produce identical state
- **Late Arrivals**: Out-of-order events must update historical state correctly
- **PII Protection**: 100% masking required
- **Adversarial Resistance**: 200/200 payloads must be blocked

## Token Budget Enforcement

- Single-Pass prompts: Max 500 tokens
- Self-Consistency prompts: Max 1500 tokens  
- Tree-of-Thought prompts: Max 5000 tokens
- Debugging loops: Max 3 iterations
