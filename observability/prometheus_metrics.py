"""
Prometheus Metrics for PIT Feature Store
Captures ingestion lag, checkpoint duration, query latency, and data drift.
"""

import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from contextlib import contextmanager

try:
    from prometheus_client import Counter, Histogram, Gauge, start_http_server, REGISTRY
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


class PITMetrics:
    """Prometheus metrics collector for PIT feature store."""
    
    def __init__(self, port: int = 9090):
        self.port = port
        self._initialized = False
        
        if PROMETHEUS_AVAILABLE:
            # Ingestion metrics
            self.events_ingested = Counter(
                'pit_events_ingested_total',
                'Total number of events ingested',
                ['source', 'event_type']
            )
            self.ingestion_lag = Gauge(
                'pit_ingestion_lag_seconds',
                'Current ingestion lag in seconds',
                ['source']
            )
            
            # Flink metrics
            self.checkpoint_duration = Histogram(
                'pit_flink_checkpoint_duration_seconds',
                'Flink checkpoint duration',
                buckets=[1, 5, 10, 30, 60, 120, 300]
            )
            self.checkpoint_success = Counter(
                'pit_flink_checkpoints_total',
                'Total Flink checkpoints',
                ['status']
            )
            
            # ClickHouse metrics
            self.query_latency = Histogram(
                'pit_clickhouse_query_latency_seconds',
                'ClickHouse query latency',
                buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
            )
            self.pit_query_count = Counter(
                'pit_queries_total',
                'Total PIT queries',
                ['query_type']
            )
            
            # Data quality metrics
            self.late_events = Counter(
                'pit_late_events_total',
                'Total late events received',
                ['lateness_bucket']
            )
            self.duplicate_events = Counter(
                'pit_duplicate_events_total',
                'Total duplicate events detected'
            )
            
            # Security metrics
            self.pii_tokenizations = Counter(
                'pit_pii_tokenizations_total',
                'Total PII tokenization operations'
            )
            self.security_alerts = Counter(
                'pit_security_alerts_total',
                'Total security alerts',
                ['alert_type']
            )
            
            # Throughput metrics
            self.throughput = Gauge(
                'pit_throughput_events_per_second',
                'Current throughput in events per second'
            )
            
            self._initialized = True
    
    def start_server(self):
        """Start Prometheus HTTP server."""
        if PROMETHEUS_AVAILABLE and not self._initialized:
            start_http_server(self.port)
            self._initialized = True
    
    @contextmanager
    def track_latency(self, metric_name: str, labels: Optional[Dict] = None):
        """Context manager to track operation latency."""
        start = time.time()
        try:
            yield
        finally:
            latency = time.time() - start
            if PROMETHEUS_AVAILABLE and self._initialized:
                if metric_name == 'query':
                    self.query_latency.observe(latency)
    
    def record_event_ingested(self, source: str, event_type: str):
        """Record an ingested event."""
        if PROMETHEUS_AVAILABLE and self._initialized:
            self.events_ingested.labels(source=source, event_type=event_type).inc()
    
    def record_checkpoint(self, duration: float, success: bool):
        """Record a Flink checkpoint."""
        if PROMETHEUS_AVAILABLE and self._initialized:
            self.checkpoint_duration.observe(duration)
            status = 'success' if success else 'failure'
            self.checkpoint_success.labels(status=status).inc()
    
    def record_pit_query(self, query_type: str, latency: float):
        """Record a PIT query."""
        if PROMETHEUS_AVAILABLE and self._initialized:
            self.pit_query_count.labels(query_type=query_type).inc()
            self.query_latency.observe(latency)
    
    def record_late_event(self, lateness_seconds: float):
        """Record a late event with lateness bucket."""
        if PROMETHEUS_AVAILABLE and self._initialized:
            if lateness_seconds < 10:
                bucket = '0-10s'
            elif lateness_seconds < 60:
                bucket = '10-60s'
            else:
                bucket = '60s+'
            self.late_events.labels(lateness_bucket=bucket).inc()
    
    def record_duplicate_event(self):
        """Record a duplicate event."""
        if PROMETHEUS_AVAILABLE and self._initialized:
            self.duplicate_events.inc()
    
    def record_pii_tokenization(self):
        """Record a PII tokenization operation."""
        if PROMETHEUS_AVAILABLE and self._initialized:
            self.pii_tokenizations.inc()
    
    def record_security_alert(self, alert_type: str):
        """Record a security alert."""
        if PROMETHEUS_AVAILABLE and self._initialized:
            self.security_alerts.labels(alert_type=alert_type).inc()
    
    def update_throughput(self, events_per_second: float):
        """Update throughput gauge."""
        if PROMETHEUS_AVAILABLE and self._initialized:
            self.throughput.set(events_per_second)
    
    def update_ingestion_lag(self, source: str, lag_seconds: float):
        """Update ingestion lag gauge."""
        if PROMETHEUS_AVAILABLE and self._initialized:
            self.ingestion_lag.labels(source=source).set(lag_seconds)


# Global metrics instance
_metrics: Optional[PITMetrics] = None


def get_metrics() -> PITMetrics:
    """Get or create global metrics instance."""
    global _metrics
    if _metrics is None:
        _metrics = PITMetrics()
    return _metrics


if __name__ == "__main__":
    metrics = get_metrics()
    metrics.start_server()
    print(f"Prometheus metrics server started on port {metrics.port}")
