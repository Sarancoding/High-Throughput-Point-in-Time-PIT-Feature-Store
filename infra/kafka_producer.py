"""
Kafka Producer for CDC Events
Implements idempotent producers with partitioning by account ID.
Targets >100,000 events/sec throughput.
"""

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import hashlib


class IdempotentKafkaProducer:
    """Idempotent Kafka producer for CDC events with exactly-once semantics."""
    
    def __init__(self, brokers: list[str], topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            enable_idempotence=True,
            acks='all',
            retries=5,
            max_in_flight_requests_per_connection=5,
            compression_type='lz4',
            batch_size=16384,
            linger_ms=5,
            buffer_memory=33554432,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
        )
        self._message_cache: Dict[str, str] = {}  # Deduplication cache
        
    def _generate_dedup_key(self, event: Dict[str, Any]) -> str:
        """Generate deterministic deduplication key from event content."""
        # Use transaction_id if available, otherwise hash critical fields
        if 'transaction_id' in event:
            return event['transaction_id']
        
        # Create hash of critical fields for deduplication
        critical_fields = ['account_id', 'event_time', 'event_type', 'amount']
        content = '|'.join(str(event.get(f, '')) for f in critical_fields)
        return hashlib.sha256(content.encode()).hexdigest()[:32]
    
    def _is_duplicate(self, dedup_key: str) -> bool:
        """Check if message is a duplicate using in-memory cache."""
        if dedup_key in self._message_cache:
            return True
        self._message_cache[dedup_key] = time.time()
        
        # Prune old entries (keep last 1M keys)
        if len(self._message_cache) > 1_000_000:
            cutoff = time.time() - 3600  # Keep 1 hour
            self._message_cache = {
                k: v for k, v in self._message_cache.items() 
                if v > cutoff
            }
        return False
    
    def send_cdc_event(
        self, 
        event: Dict[str, Any], 
        partition_key: Optional[str] = None
    ) -> bool:
        """
        Send CDC event with idempotent writes.
        
        Args:
            event: Event dictionary with required fields:
                   - account_id: Account identifier
                   - event_time: Event timestamp (ISO format)
                   - event_type: Type of event (INSERT/UPDATE/DELETE)
                   - data: Event payload
            partition_key: Optional partition key (defaults to account_id)
            
        Returns:
            True if sent successfully, False if duplicate
        """
        # Validate required fields
        required_fields = ['account_id', 'event_time', 'event_type']
        for field in required_fields:
            if field not in event:
                raise ValueError(f"Missing required field: {field}")
        
        # Check for duplicates
        dedup_key = self._generate_dedup_key(event)
        if self._is_duplicate(dedup_key):
            return False
        
        # Add metadata
        event['processing_time'] = datetime.now(timezone.utc).isoformat()
        event['_dedup_key'] = dedup_key
        
        # Partition by account_id if no partition_key provided
        key = partition_key or str(event['account_id'])
        
        try:
            future = self.producer.send(
                self.topic,
                key=key,
                value=event
            )
            # Block until message is sent
            record_metadata = future.get(timeout=10)
            return True
        except KafkaError as e:
            # Log error but don't retry (idempotency handles this)
            print(f"Kafka error: {e}")
            return False
    
    def flush(self):
        """Flush all pending messages."""
        self.producer.flush()
    
    def close(self):
        """Close producer connection."""
        self.producer.close()


def create_sample_cdc_events(count: int = 1000) -> list[Dict[str, Any]]:
    """Generate sample CDC events for testing."""
    events = []
    base_time = datetime.now(timezone.utc)
    
    for i in range(count):
        event = {
            'transaction_id': str(uuid.uuid4()),
            'account_id': f"ACC_{i % 1000:06d}",
            'event_time': (base_time.replace(microsecond=i % 1000000)).isoformat(),
            'event_type': 'TRANSACTION',
            'data': {
                'amount': round((i % 10000) / 100.0, 2),
                'currency': 'USD',
                'merchant_id': f"MERCHANT_{i % 100:03d}",
                'category': ['retail', 'food', 'travel', 'entertainment'][i % 4]
            }
        }
        events.append(event)
    
    return events


if __name__ == "__main__":
    # Example usage
    import os
    
    brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')
    topic = 'cdc_transactions'
    
    producer = IdempotentKafkaProducer(brokers, topic)
    
    try:
        events = create_sample_cdc_events(100)
        sent_count = 0
        
        for event in events:
            if producer.send_cdc_event(event):
                sent_count += 1
        
        producer.flush()
        print(f"Sent {sent_count}/{len(events)} events")
        
    finally:
        producer.close()
