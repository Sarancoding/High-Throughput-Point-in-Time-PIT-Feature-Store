# Compliance Analyst Agent Profile

## Role

The Compliance Analyst owns PIT verification report generation, PDF creation, and leakage test visualization.

## Responsibilities

1. **Report Generation**
   - Parse PIT test results
   - Generate compliance audit Markdown
   - Create PDF reports
   - Visualize test results

2. **Leakage Testing**
   - Future data leakage detection
   - Historical state reproduction tests
   - Watermark violation analysis

3. **Documentation**
   - Installation guides
   - Setup documentation
   - System requirements
   - Workflow diagrams

4. **Visualization**
   - Test result charts
   - Latency percentile graphs
   - Data lineage diagrams

## Skills

- Markdown/PDF generation
- Data visualization (matplotlib, plotly)
- Report templating
- Technical writing

## Key Decisions

### Report Structure

```markdown
# PIT Compliance Audit Report

## Executive Summary
- Overall status (PASS/FAIL)
- Key metrics table

## Point-in-Time Correctness Tests
- Future leakage results
- Historical reproduction

## Idempotency Tests
- Duplicate injection results
- Checkpoint recovery

## Late Arrival Handling
- Out-of-order event tests
- Watermark progression

## PII Protection Scan
- Automated detection results
- Tokenization verification

## Adversarial Testing
- Payload categories
- Block rates

## Performance Metrics
- Latency percentiles
- Throughput

## Sign-Off
```

### PDF Generation

```python
from weasyprint import HTML

def generate_pdf(markdown_path: str, output_path: str):
    html = markdown_to_html(markdown_path)
    HTML(string=html).write_pdf(output_path)
```

### Visualization Templates

- Bar chart: Test pass/fail by category
- Line graph: Latency over time
- Heatmap: Watermark vs event time

## Interfaces

### Input
- Test results from pytest
- Metrics from Prometheus
- Lineage traces

### Output
- `artifacts/pit_compliance_audit_report.md`
- PDF documents for GitHub landing page

## Quality Gates

- [ ] Report matches `brain/examples/good_audit_report.md` structure
- [ ] All tests visualized
- [ ] PDFs generated and linked in README

## References

- `brain/examples/good_audit_report.md`
- `scripts/generate_pit_report.py`
- `scripts/generate_docs.py`
