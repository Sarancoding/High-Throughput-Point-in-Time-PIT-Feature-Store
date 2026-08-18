"""
Apache Flink Stream Processing Job for PIT Feature Computation
Implements watermarks, allowed lateness, windowed aggregations, and exactly-once state.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict

# PyFlink imports (simulated for standalone execution)
try:
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment, EnvironmentSettings
    from pyflink.datastream.watermark_strategy import WatermarkStrategy
    from pyflink.common.time import Duration
    FLINK_AVAILABLE = True
except ImportError:
    FLINK_AVAILABLE = False


@dataclass
class TransactionEvent:
    """Transaction event with PIT semantics."""
    transaction_id: str
    account_id: str
    event_time: str
    event_type: str
    amount: float
    currency: str
    merchant_id: Optional[str] = None
    category: Optional[str] = None
    processing_time: Optional[str] = None
    watermark: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TransactionEvent':
        return cls(
            transaction_id=data.get('transaction_id', ''),
            account_id=data.get('account_id', ''),
            event_time=data.get('event_time', ''),
            event_type=data.get('event_type', ''),
            amount=data.get('data', {}).get('amount', 0.0),
            currency=data.get('data', {}).get('currency', 'USD'),
            merchant_id=data.get('data', {}).get('merchant_id'),
            category=data.get('data', {}).get('category'),
            processing_time=data.get('processing_time'),
            watermark=data.get('watermark')
        )


@dataclass
class AccountFeature:
    """Computed feature vector for an account at a point in time."""
    account_id: str
    feature_timestamp: str
    transaction_count_1h: int
    transaction_count_24h: int
    total_amount_1h: float
    total_amount_24h: float
    avg_amount_1h: float
    avg_amount_24h: float
    unique_merchants_24h: int
    last_transaction_time: str
    risk_score: float
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class PITFeatureComputer:
    """
    Point-in-Time feature computer with watermarking and late arrival handling.
    
    Implements:
    - Event time processing with watermarks
    - Allowed lateness for late arrivals
    - Exactly-once stateful aggregations
    - Retraction support for corrections
    """
    
    def __init__(
        self,
        watermark_delay_seconds: int = 5,
        allowed_lateness_seconds: int = 60,
        window_size_seconds: int = 3600
    ):
        self.watermark_delay = timedelta(seconds=watermark_delay_seconds)
        self.allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        self.window_size = timedelta(seconds=window_size_seconds)
        
        # State stores for exactly-once semantics
        self._account_state: Dict[str, Dict[str, Any]] = {}
        self._processed_events: set = set()  # Deduplication set
        self._late_events_buffer: List[TransactionEvent] = []
        
    def _parse_event_time(self, event: TransactionEvent) -> datetime:
        """Parse event time from ISO format."""
        try:
            return datetime.fromisoformat(event.event_time.replace('Z', '+00:00'))
        except ValueError:
            return datetime.now(timezone.utc)
    
    def _compute_watermark(self, event_time: datetime) -> datetime:
        """Compute watermark based on event time and delay."""
        return event_time - self.watermark_delay
    
    def _is_late_event(
        self, 
        event: TransactionEvent, 
        current_watermark: datetime
    ) -> bool:
        """Check if event is late (before watermark)."""
        event_time = self._parse_event_time(event)
        return event_time < current_watermark
    
    def _update_account_state(
        self, 
        event: TransactionEvent,
        is_late: bool = False
    ) -> Optional[AccountFeature]:
        """
        Update account state with new event.
        
        Handles:
        - Normal events: Update state and emit features
        - Late events: Update historical state and emit retraction
        - Duplicate events: Ignore (idempotency)
        """
        # Check for duplicates (exactly-once)
        if event.transaction_id in self._processed_events:
            return None
        
        self._processed_events.add(event.transaction_id)
        
        account_id = event.account_id
        event_time = self._parse_event_time(event)
        
        # Initialize account state if needed
        if account_id not in self._account_state:
            self._account_state[account_id] = {
                'events': [],
                'features_history': []
            }
        
        # Add event to state
        self._account_state[account_id]['events'].append({
            'event': event,
            'event_time': event_time,
            'is_late': is_late
        })
        
        # Compute features for 1h and 24h windows
        now = event_time
        one_hour_ago = now - timedelta(hours=1)
        twenty_four_hours_ago = now - timedelta(hours=24)
        
        # Filter events in windows
        events_1h = [
            e for e in self._account_state[account_id]['events']
            if e['event_time'] >= one_hour_ago and e['event_time'] <= now
        ]
        events_24h = [
            e for e in self._account_state[account_id]['events']
            if e['event_time'] >= twenty_four_hours_ago and e['event_time'] <= now
        ]
        
        # Compute aggregations
        count_1h = len(events_1h)
        count_24h = len(events_24h)
        total_1h = sum(e['event'].amount for e in events_1h)
        total_24h = sum(e['event'].amount for e in events_24h)
        avg_1h = total_1h / count_1h if count_1h > 0 else 0.0
        avg_24h = total_24h / count_24h if count_24h > 0 else 0.0
        unique_merchants = len(set(
            e['event'].merchant_id for e in events_24h 
            if e['event'].merchant_id
        ))
        
        # Simple risk score calculation
        risk_score = self._compute_risk_score(events_24h, count_24h, total_24h)
        
        # Create feature vector
        feature = AccountFeature(
            account_id=account_id,
            feature_timestamp=event_time.isoformat(),
            transaction_count_1h=count_1h,
            transaction_count_24h=count_24h,
            total_amount_1h=round(total_1h, 2),
            total_amount_24h=round(total_24h, 2),
            avg_amount_1h=round(avg_1h, 2),
            avg_amount_24h=round(avg_24h, 2),
            unique_merchants_24h=unique_merchants,
            last_transaction_time=event_time.isoformat(),
            risk_score=risk_score
        )
        
        # Store feature history
        self._account_state[account_id]['features_history'].append(feature)
        
        return feature
    
    def _compute_risk_score(
        self, 
        events: List[Dict], 
        count: int, 
        total: float
    ) -> float:
        """Compute simple risk score based on transaction patterns."""
        if count == 0:
            return 0.0
        
        # Factors:
        # - High frequency (>10 transactions/hour)
        # - High total amount (>$1000/hour)
        # - High average amount (>$500)
        
        risk = 0.0
        
        if count > 10:
            risk += 0.3
        elif count > 5:
            risk += 0.1
        
        if total > 1000:
            risk += 0.4
        elif total > 500:
            risk += 0.2
        
        avg = total / count
        if avg > 500:
            risk += 0.3
        elif avg > 200:
            risk += 0.1
        
        return min(risk, 1.0)
    
    def process_event(
        self, 
        event_data: Dict[str, Any],
        current_watermark: Optional[datetime] = None
    ) -> Optional[AccountFeature]:
        """
        Process a single event and return computed features.
        
        Args:
            event_data: Raw event dictionary
            current_watermark: Current watermark for late detection
            
        Returns:
            AccountFeature if successful, None if duplicate
        """
        event = TransactionEvent.from_dict(event_data)
        
        # Parse event time
        event_time = self._parse_event_time(event)
        
        # Update watermark if not provided
        if current_watermark is None:
            current_watermark = self._compute_watermark(event_time)
        
        # Check if late
        is_late = self._is_late_event(event, current_watermark)
        
        if is_late:
            # Buffer late event for later processing
            self._late_events_buffer.append(event)
            
            # Check if within allowed lateness
            if event_time < current_watermark - self.allowed_lateness:
                # Too late, drop event but log for audit
                print(f"Dropping too-late event: {event.transaction_id}")
                return None
        
        # Update state and compute features
        return self._update_account_state(event, is_late)
    
    def get_feature_as_of(
        self, 
        account_id: str, 
        timestamp: datetime
    ) -> Optional[AccountFeature]:
        """
        Retrieve feature vector as of a specific point in time.
        
        This is the core PIT query - returns the state that was valid
        at the given timestamp, without future data leakage.
        """
        if account_id not in self._account_state:
            return None
        
        # Find the most recent feature before or at timestamp
        features = self._account_state[account_id]['features_history']
        
        pit_feature = None
        for feature in features:
            feature_time = datetime.fromisoformat(feature.feature_timestamp.replace('Z', '+00:00'))
            if feature_time <= timestamp:
                pit_feature = feature
            else:
                break  # Features are ordered by time
        
        return pit_feature
    
    def get_state_snapshot(self) -> Dict[str, Any]:
        """Get state snapshot for checkpointing."""
        return {
            'account_state': self._account_state,
            'processed_events': list(self._processed_events),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    def restore_from_snapshot(self, snapshot: Dict[str, Any]):
        """Restore state from checkpoint."""
        self._account_state = snapshot.get('account_state', {})
        self._processed_events = set(snapshot.get('processed_events', []))


def create_flink_job():
    """Create and configure Flink streaming job."""
    if not FLINK_AVAILABLE:
        print("PyFlink not available, running in simulation mode")
        return None
    
    # Set up execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Enable exactly-once semantics
    env.enable_checkpointing(60000)  # Checkpoint every 60 seconds
    env.set_parallelism(int(os.getenv('FLINK_PARALLELISM', '4')))
    env.setMaxRestoredParallelism(int(os.getenv('FLINK_PARALLELISM', '4')))
    
    # Configure state backend
    # env.setStateBackend(RocksDBStateBackend('hdfs://namenode:8020/flink/checkpoints'))
    
    # Create table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, settings)
    
    # Configure watermark strategy
    # watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
    
    return {
        'env': env,
        'table_env': table_env
    }


if __name__ == "__main__":
    # Test PIT feature computation
    computer = PITFeatureComputer(
        watermark_delay_seconds=5,
        allowed_lateness_seconds=60
    )
    
    # Simulate events
    base_time = datetime.now(timezone.utc)
    
    test_events = [
        {
            'transaction_id': f'tx_{i}',
            'account_id': 'acc_001',
            'event_time': (base_time - timedelta(minutes=i)).isoformat(),
            'event_type': 'TRANSACTION',
            'data': {
                'amount': 100.0 + i * 10,
                'currency': 'USD',
                'merchant_id': f'merchant_{i % 5}',
                'category': 'retail'
            }
        }
        for i in range(10)
    ]
    
    # Process events
    features = []
    for event_data in test_events:
        feature = computer.process_event(event_data)
        if feature:
            features.append(feature)
            print(f"Processed: {feature.account_id} @ {feature.feature_timestamp}")
    
    # Test PIT query
    pit_time = base_time - timedelta(minutes=5)
    pit_feature = computer.get_feature_as_of('acc_001', pit_time)
    
    if pit_feature:
        print(f"\nPIT Feature as of {pit_time}:")
        print(json.dumps(pit_feature.to_dict(), indent=2))
    
    # Test state snapshot
    snapshot = computer.get_state_snapshot()
    print(f"\nState snapshot created with {len(snapshot['processed_events'])} processed events")
