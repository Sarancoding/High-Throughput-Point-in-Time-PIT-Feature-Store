"""
PIT Correctness Tests
Verifies zero future data leakage in point-in-time queries.
"""

import pytest
from datetime import datetime, timedelta, timezone
import sys
sys.path.insert(0, '/workspace')

from stream.flink_job import PITFeatureComputer, TransactionEvent


class TestPITCorrectness:
    """Test suite for PIT correctness verification."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.computer = PITFeatureComputer(
            watermark_delay_seconds=5,
            allowed_lateness_seconds=60
        )
        self.base_time = datetime.now(timezone.utc)
    
    def test_no_future_data_leakage(self):
        """Verify PIT query returns only past state, not future data."""
        # Create events at different times
        events = [
            {
                'transaction_id': f'tx_{i}',
                'account_id': 'acc_001',
                'event_time': (self.base_time - timedelta(hours=i)).isoformat(),
                'event_type': 'TRANSACTION',
                'data': {'amount': 100.0 * i, 'currency': 'USD'}
            }
            for i in range(1, 6)  # 1-5 hours ago
        ]
        
        # Process all events
        for event in events:
            self.computer.process_event(event)
        
        # Query PIT at 3 hours ago
        pit_time = self.base_time - timedelta(hours=3)
        pit_feature = self.computer.get_feature_as_of('acc_001', pit_time)
        
        # Verify no future data (events from 1-2 hours ago should not be included)
        assert pit_feature is not None
        # At 3 hours ago, only events from 3+ hours ago should be visible
        assert pit_feature.transaction_count_24h <= 3
    
    def test_pit_query_before_any_events(self):
        """Verify PIT query before any events returns None or empty."""
        pit_time = self.base_time - timedelta(days=1)
        pit_feature = self.computer.get_feature_as_of('acc_001', pit_time)
        
        # Should return None or feature with zero counts
        assert pit_feature is None or pit_feature.transaction_count_24h == 0
    
    def test_pit_query_after_all_events(self):
        """Verify PIT query after all events includes all data."""
        events = [
            {
                'transaction_id': f'tx_{i}',
                'account_id': 'acc_001',
                'event_time': (self.base_time - timedelta(hours=i)).isoformat(),
                'event_type': 'TRANSACTION',
                'data': {'amount': 100.0 * i, 'currency': 'USD'}
            }
            for i in range(1, 6)
        ]
        
        for event in events:
            self.computer.process_event(event)
        
        # Query after all events
        pit_time = self.base_time
        pit_feature = self.computer.get_feature_as_of('acc_001', pit_time)
        
        assert pit_feature is not None
        assert pit_feature.transaction_count_24h == 5


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
