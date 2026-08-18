.PHONY: all setup-infra build-stream-job apply-security instrument-metrics run-pit-tests generate-audit push-to-github clean

all: setup-infra build-stream-job apply-security instrument-metrics run-pit-tests generate-audit

# Phase 2: Infrastructure Setup
setup-infra:
	@echo "Setting up infrastructure..."
	python -m infra.schema_registry
	docker-compose up -d kafka clickhouse prometheus grafana redis
	@echo "Infrastructure ready"

# Phase 3: Build Stream Processing Job
build-stream-job:
	@echo "Building Flink stream job..."
	python -m stream.flink_job --deploy
	@echo "Stream job deployed"

# Phase 3: Apply Security Measures
apply-security:
	@echo "Applying security measures..."
	python -m security.pii_tokenizer --init
	python -m security.audit_logger --init
	@echo "Security measures applied"

# Phase 3: Instrument Metrics
instrument-metrics:
	@echo "Instrumenting Prometheus metrics..."
	python -m observability.prometheus_metrics --register
	@echo "Metrics instrumented"

# Phase 4: Run PIT Tests
run-pit-tests:
	@echo "Running PIT correctness tests..."
	pytest tests/test_pit_correctness.py -v
	pytest tests/test_idempotency.py -v
	pytest tests/test_late_arrivals.py -v
	pytest tests/test_security.py -v
	@echo "All tests passed"

# Phase 5: Generate Audit Report
generate-audit:
	@echo "Generating compliance audit report..."
	python scripts/generate_pit_report.py
	python scripts/generate_docs.py
	@echo "Audit report generated"

# Phase 6: Push to GitHub
push-to-github:
	@echo "Pushing to GitHub..."
	bash scripts/push_to_github.sh
	@echo "Pushed to GitHub"

clean:
	docker-compose down
	rm -rf results/* artifacts/*.pdf
