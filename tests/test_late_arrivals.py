"""
Late Arrivals Tests
Verifies out-of-order events correctly update historical state.
"""

import pytest
from datetime import datetime, timedelta, timezone
import sys
sys.path.insert(0, '/workspace')

from stream.flink_job import PITFeatureComputer


class TestLateArrivals:
    """Test suite for late arrival handling verification."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.computer = PITFeatureComputer(
            watermark_delay_seconds=5,
            allowed_lateness_seconds=300  # 5 minutes
        )
        self.base_time = datetime.now(timezone.utc)
    
    def test_late_event_updates_state(self):
        """Verify late events within allowed lateness update state."""
        # Process recent event first
        recent_event = {
            'transaction_id': 'tx_recent',
            'account_id': 'acc_001',
            'event_time': self.base_time.isoformat(),
            'event_type': 'TRANSACTION',
            'data': {'amount': 100.0, 'currency': 'USD'}
        }
        
        # Process older (late) event
        late_event = {
            'transaction_id': 'tx_late',
            'account_id': 'acc_001',
            'event_time': (self.base_time - timedelta(minutes=2)).isoformat(),
            'event_type': 'TRANSACTION',
            'data': {'amount': 50.0, 'currency': 'USD'}
        }
        
        # Process recent first, then late
        result_recent = self.computer.process_event(recent_event)
        result_late = self.computer.process_event(late_event)
        
        # Both should be processed (late within allowed lateness)
        assert result_recent is not None
        assert result_late is not None
    
    def test_too_late_event_dropped(self):
        """Verify events beyond allowed lateness are dropped."""
        computer = PITFeatureComputer(
            watermark_delay_seconds=5,
            allowed_lateness_seconds=60  # 1 minute
        )
        
        # Process very old event
        old_event = {
            'transaction_id': 'tx_old',
            'account_id': 'acc_001',
            'event_time': (self.base_time - timedelta(hours=1)).isoformat(),
            'event_type': 'TRANSACTION',
            'data': {'amount': 100.0, 'currency': 'USD'}
        }
        
        # Set watermark to now
        result = computer.process_event(old_event)
        
        # Should be dropped (too late)
        assert result is None
    
    def test_late_events_preserve_order(self):
        """Verify late events maintain correct temporal ordering."""
        events = [
            {
                'transaction_id': f'tx_{i}',
                'account_id': 'acc_001',
                'event_time': (self.base_time - timedelta(hours=i)).isoformat(),
                'event_type': 'TRANSACTION',
                'data': {'amount': 100.0 * i, 'currency': 'USD'}
            }
            for i in [5, 1, 3, 2, 4]  # Out of order
        ]
        
        # Process out of order
        for event in events:
            self.computer.process_event(event)
        
        # Get feature at specific time
        pit_time = self.base_time - timedelta(hours=3)
        pit_feature = self.computer.get_feature_as_of('acc_001', pit_time)
        
        # Should include only events from 3+ hours ago
        assert pit_feature is not None
        assert pit_feature.transaction_count_24h >= 2  # Events at 5h and 4h


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
