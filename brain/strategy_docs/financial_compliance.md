# Financial Compliance Requirements

## Regulatory Frameworks

This feature store must comply with:

### SEC (Securities and Exchange Commission)

- **Rule 17a-4**: Record retention for 6+ years
- **Regulation SCI**: Systems Compliance and Integrity
- **CAT (Consolidated Audit Trail)**: Complete audit trail of all orders and trades

### FINRA (Financial Industry Regulatory Authority)

- **Rule 4511**: Books and records requirements
- **Rule 6800 Series**: Order audit trail system
- **Data Integrity**: Accurate, complete, and timely data

### GDPR (General Data Protection Regulation)

- **Article 25**: Data protection by design and by default
- **Article 30**: Records of processing activities
- **Right to Erasure**: Ability to delete PII upon request

## Feature Store Compliance Requirements

### 1. Audit Trail

Every feature computation must be traceable:

```json
{
  "feature_id": "account_hourly_volume",
  "account_id": "A123",
  "computation_timestamp": "2024-01-15T10:05:00Z",
  "input_events": ["tx_001", "tx_002", "tx_003"],
  "pipeline_version": "v1.2.3",
  "operator": "flink_job_001",
  "checkpoint_id": "chk_789"
}
```

### 2. Data Lineage

Track data from source to feature vector:

```
Source DB -> CDC -> Kafka Topic -> Flink Job -> ClickHouse Table -> Feature Vector -> ML Model
   |           |         |              |             |                 |              |
  schema     offset    partition     operator      version         access_log    prediction_id
```

### 3. Reproducibility

Any historical feature value must be reproducible:

- Raw events stored immutably
- Pipeline code versioned
- Configuration snapshotted
- Dependencies pinned

### 4. Access Control

- **RBAC**: Role-based access control for all data assets
- **Least Privilege**: Minimum necessary permissions
- **Audit Logging**: All access logged with timestamp and user

## Prohibited Practices

### ❌ Future Data Leakage

Never compute features using information not available at the prediction timestamp.

### ❌ PII in Feature Vectors

Never expose raw PII in features sent to ML models:

```python
# WRONG - exposes PII
features = {
    "account_holder_ssn": "123-45-6789",
    "transaction_amount": 10000
}

# CORRECT - tokenized
features = {
    "account_holder_token": "tok_a1b2c3d4",
    "transaction_amount": 10000
}
```

### ❌ Non-Idempotent Aggregations

Never allow duplicate events to corrupt state.

### ❌ Unlogged Data Transformations

Never modify data without logging the transformation.

## Compliance Checklist

- [ ] All features have audit trails
- [ ] Data lineage is traceable end-to-end
- [ ] PII is tokenized before feature assembly
- [ ] Historical states are reproducible
- [ ] Access logs capture all feature retrievals
- [ ] Retention policies enforced
- [ ] Encryption at rest and in transit
- [ ] Regular compliance audits scheduled

## References

- `brain/strategy_docs/pit_semantics.md` - Preventing future leakage
- `brain/strategy_docs/pii_protection.md` - PII handling
- `brain/sops/graph_orchestration.md` - Lineage tracking
- `brain/examples/good_audit_report.md` - Audit report format
