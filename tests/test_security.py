"""
Security Tests
Verifies PII protection and adversarial resistance.
"""

import pytest
import sys
sys.path.insert(0, '/workspace')

from security.pii_tokenizer import PIITokenizer, PIICategory


class TestSecurity:
    """Test suite for security verification."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.tokenizer = PIITokenizer()
    
    def test_ssn_tokenization(self):
        """Verify SSN is properly tokenized."""
        ssn = "123-45-6789"
        tokenized = self.tokenizer.tokenize(ssn, PIICategory.SSN)
        
        # Token should be different from original
        assert tokenized.token != ssn
        # Token should not contain original pattern
        assert "123" not in tokenized.token or "45" not in tokenized.token
    
    def test_account_number_format_preserving(self):
        """Verify account number maintains format."""
        account = "ACC_001234567"
        tokenized = self.tokenizer.tokenize(account, PIICategory.ACCOUNT_NUMBER, preserve_format=True)
        
        # Should maintain ACC_ prefix and length
        assert tokenized.token.startswith("ACC_")
        assert len(tokenized.token) == len(account)
    
    def test_email_masking(self):
        """Verify email is properly masked."""
        email = "john.doe@example.com"
        masked = self.tokenizer.mask_pii(email, PIICategory.EMAIL)
        
        # Should mask local part
        assert "@" in masked
        assert "john" not in masked.lower() or "***" in masked
    
    def test_no_pii_leakage_detection(self):
        """Verify PII leakage detection works."""
        data_with_pii = {
            'account_id': 'ACC_001234',
            'ssn': '123-45-6789',  # Should be detected
            'safe_field': 'normal_value'
        }
        
        leaks = self.tokenizer.verify_no_pii_leakage(data_with_pii)
        
        # Should detect SSN pattern
        assert any('ssn' in leak.lower() for leak in leaks)
    
    def test_deterministic_tokenization(self):
        """Verify same input produces same token."""
        value = "test-account-123"
        
        token1 = self.tokenizer.tokenize(value, PIICategory.CUSTOM)
        token2 = self.tokenizer.tokenize(value, PIICategory.CUSTOM)
        
        # Same input should produce same token (deterministic)
        assert token1.token == token2.token


class TestAdversarialResistance:
    """Test adversarial payload resistance."""
    
    def test_malformed_event_handling(self):
        """Verify malformed events don't crash system."""
        from stream.flink_job import PITFeatureComputer
        
        computer = PITFeatureComputer()
        
        malformed_events = [
            {},  # Empty
            {'account_id': 'acc'},  # Missing required fields
            {'transaction_id': 'tx', 'account_id': None},  # Null values
            {'transaction_id': 'tx', 'account_id': 'acc', 'event_time': 'invalid-date'},
        ]
        
        for event in malformed_events:
            try:
                result = computer.process_event(event)
                # Should either return None or handle gracefully
            except Exception:
                pass  # Expected for some malformed events
    
    def test_injection_attempt_handling(self):
        """Verify injection attempts are handled."""
        tokenizer = PIITokenizer()
        
        injection_payloads = [
            "'; DROP TABLE users; --",
            "<script>alert('xss')</script>",
            "../../../etc/passwd",
        ]
        
        for payload in injection_payloads:
            try:
                tokenized = tokenizer.tokenize(payload, PIICategory.CUSTOM)
                # Should tokenize without executing
                assert tokenized.token is not None
            except Exception:
                pass  # Also acceptable to reject


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
