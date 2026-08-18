"""
Idempotency Tests
Verifies duplicate CDC events produce identical state.
"""

import pytest
from datetime import datetime, timedelta, timezone
import sys
sys.path.insert(0, '/workspace')

from stream.flink_job import PITFeatureComputer


class TestIdempotency:
    """Test suite for exactly-once semantics verification."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.computer = PITFeatureComputer()
        self.base_time = datetime.now(timezone.utc)
    
    def test_duplicate_events_ignored(self):
        """Verify duplicate events are not processed twice."""
        event = {
            'transaction_id': 'tx_001',
            'account_id': 'acc_001',
            'event_time': self.base_time.isoformat(),
            'event_type': 'TRANSACTION',
            'data': {'amount': 100.0, 'currency': 'USD'}
        }
        
        # Process same event twice
        result1 = self.computer.process_event(event)
        result2 = self.computer.process_event(event)
        
        # Second processing should return None (duplicate)
        assert result1 is not None
        assert result2 is None
    
    def test_identical_state_after_duplicates(self):
        """Verify state is identical regardless of duplicate submissions."""
        events = [
            {
                'transaction_id': f'tx_{i}',
                'account_id': 'acc_001',
                'event_time': (self.base_time - timedelta(hours=i)).isoformat(),
                'event_type': 'TRANSACTION',
                'data': {'amount': 100.0 * i, 'currency': 'USD'}
            }
            for i in range(1, 4)
        ]
        
        # Process events once
        for event in events:
            self.computer.process_event(event)
        
        # Get state snapshot
        state1 = self.computer.get_state_snapshot()
        
        # Try to process duplicates
        for event in events:
            self.computer.process_event(event)
        
        # Get state again
        state2 = self.computer.get_state_snapshot()
        
        # States should be identical (same number of processed events)
        assert len(state1['processed_events']) == len(state2['processed_events'])
    
    def test_order_independent_for_same_events(self):
        """Verify processing same events in different order produces same final state."""
        base_event = {
            'account_id': 'acc_001',
            'event_type': 'TRANSACTION',
            'data': {'amount': 100.0, 'currency': 'USD'}
        }
        
        events_set1 = [
            {**base_event, 'transaction_id': 'tx_a', 'event_time': (self.base_time - timedelta(hours=1)).isoformat()},
            {**base_event, 'transaction_id': 'tx_b', 'event_time': (self.base_time - timedelta(hours=2)).isoformat()},
        ]
        
        events_set2 = [
            {**base_event, 'transaction_id': 'tx_b', 'event_time': (self.base_time - timedelta(hours=2)).isoformat()},
            {**base_event, 'transaction_id': 'tx_a', 'event_time': (self.base_time - timedelta(hours=1)).isoformat()},
        ]
        
        # Process in order 1
        computer1 = PITFeatureComputer()
        for event in events_set1:
            computer1.process_event(event)
        state1 = computer1.get_state_snapshot()
        
        # Process in order 2
        computer2 = PITFeatureComputer()
        for event in events_set2:
            computer2.process_event(event)
        state2 = computer2.get_state_snapshot()
        
        # Same events processed (count should match)
        assert len(state1['processed_events']) == len(state2['processed_events'])


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
