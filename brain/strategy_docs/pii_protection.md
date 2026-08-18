# PII Protection Protocol

## PII Categories

### Category 1: Direct Identifiers (STRICTLY PROHIBITED in features)

- Social Security Numbers (SSN)
- Account numbers
- Credit/debit card numbers
- Driver's license numbers
- Passport numbers

### Category 2: Quasi-Identifiers (REQUIRES MASKING)

- Full names
- Dates of birth
- ZIP codes (first 3 digits only allowed)
- Transaction timestamps (coarsen to hour for ML)

### Category 3: Transaction Identifiers (TOKENIZE)

- Transaction IDs
- Order IDs
- Trade IDs
- Reference numbers

## Tokenization Protocol

### Format-Preserving Encryption (FPE)

For fields that must maintain format (e.g., account numbers):

```python
from cryptography.fernet import Fernet

class PIITokenizer:
    def __init__(self, key: bytes):
        self.fernet = Fernet(key)
    
    def tokenize(self, pii_value: str) -> str:
        """Encrypt PII value, return base64 token"""
        return self.fernet.encrypt(pii_value.encode()).decode()
    
    def detokenize(self, token: str) -> str:
        """Decrypt token, return original PII"""
        return self.fernet.decrypt(token.encode()).decode()
```

### Hash-Based Tokenization

For one-way anonymization:

```python
import hashlib
import hmac

class PIIHasher:
    def __init__(self, secret_key: bytes):
        self.key = secret_key
    
    def hash_pii(self, pii_value: str, salt: str = "") -> str:
        """Create irreversible hash of PII"""
        message = f"{pii_value}{salt}".encode()
        return hmac.new(self.key, message, hashlib.sha256).hexdigest()
```

## Masking Rules

### At Rest (ClickHouse)

```sql
-- Store tokenized values only
CREATE TABLE features (
    account_token String,      -- NOT raw account number
    feature_name String,
    feature_value Float64,
    event_time DateTime,
    -- Raw PII columns PROHIBITED
    -- ssn String,              -- NEVER
    -- account_number String,   -- NEVER
) ENGINE = ReplacingMergeTree();
```

### In Transit (Kafka/Flink)

```python
# Before sending to Kafka
def sanitize_event(event: dict) -> dict:
    sanitized = event.copy()
    
    # Tokenize direct identifiers
    if 'ssn' in sanitized:
        sanitized['ssn_token'] = tokenizer.tokenize(sanitized['ssn'])
        del sanitized['ssn']
    
    if 'account_number' in sanitized:
        sanitized['account_token'] = tokenizer.tokenize(sanitized['account_number'])
        del sanitized['account_number']
    
    # Mask quasi-identifiers
    if 'zip_code' in sanitized:
        sanitized['zip_code'] = sanitized['zip_code'][:3] + '**'
    
    return sanitized
```

### In Feature Vectors (ML Models)

```python
# Final feature assembly - PII scan
def assemble_features(raw_features: dict) -> dict:
    PII_PATTERNS = [
        r'\d{3}-\d{2}-\d{4}',  # SSN
        r'\d{16}',             # Credit card
        r'[A-Z]{2}\d{7}',      # Passport
    ]
    
    for key, value in raw_features.items():
        for pattern in PII_PATTERNS:
            if re.match(pattern, str(value)):
                raise ValueError(f"PII detected in feature {key}")
    
    return raw_features
```

## Encryption Standards

### At Rest

- **Algorithm**: AES-256-GCM
- **Key Management**: AWS KMS / HashiCorp Vault
- **Key Rotation**: Every 90 days

### In Transit

- **Protocol**: TLS 1.3
- **Certificate Validation**: Required
- **mTLS**: For service-to-service communication

## Access Control

### RBAC Matrix

| Role | Raw PII | Tokenized PII | Features | Audit Logs |
|------|---------|---------------|----------|------------|
| Data Engineer | ❌ | ✅ | ✅ | ✅ |
| ML Engineer | ❌ | ❌ | ✅ | ✅ |
| Compliance Officer | ✅ | ✅ | ✅ | ✅ |
| Auditor | ❌ | ❌ | ✅ | ✅ |

### Access Logging

Every PII access must be logged:

```json
{
  "timestamp": "2024-01-15T10:05:00Z",
  "user_id": "compliance_officer_001",
  "action": "detokenize",
  "field": "account_token",
  "reason": "fraud_investigation_case_12345",
  "approval_ticket": "SEC-2024-001"
}
```

## Testing PII Protection

### Automated Scans

```python
def scan_for_pii(data: dict) -> list:
    """Scan data dictionary for PII patterns"""
    pii_findings = []
    patterns = {
        'ssn': r'\d{3}-\d{2}-\d{4}',
        'credit_card': r'\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}',
        'account_number': r'^[A-Z]{2}\d{6,10}$',
    }
    
    for key, value in data.items():
        for pii_type, pattern in patterns.items():
            if re.search(pattern, str(value)):
                pii_findings.append({
                    'field': key,
                    'pii_type': pii_type,
                    'value_preview': str(value)[:4] + '***'
                })
    
    return pii_findings
```

### Red Team Tests

- Attempt to inject PII into feature vectors
- Try to extract PII from logs
- Test tokenization reversibility
- Verify encryption at rest

## References

- `brain/strategy_docs/financial_compliance.md` - Regulatory requirements
- `security/pii_tokenizer.py` - Implementation
- `tests/test_security.py` - PII scan tests
