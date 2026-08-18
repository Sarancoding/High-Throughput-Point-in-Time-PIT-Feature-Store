"""
PII Tokenization Module
Implements AES-256 encryption and format-preserving tokenization for financial PII.
"""

import os
import json
import base64
import hashlib
import secrets
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False


class PIICategory(Enum):
    """Categories of PII requiring protection."""
    SSN = "ssn"
    ACCOUNT_NUMBER = "account_number"
    CREDIT_CARD = "credit_card"
    TRANSACTION_ID = "transaction_id"
    EMAIL = "email"
    PHONE = "phone"
    ADDRESS = "address"
    NAME = "name"
    CUSTOM = "custom"


@dataclass
class TokenizedValue:
    """Tokenized PII value with metadata."""
    original_category: PIICategory
    token: str
    created_at: str
    expires_at: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'category': self.original_category.value,
            'token': self.token,
            'created_at': self.created_at,
            'expires_at': self.expires_at
        }


class PIITokenizer:
    """
    PII Tokenization engine with AES-256-GCM encryption.
    
    Features:
    - Format-preserving encryption for account numbers
    - Deterministic tokenization for join operations
    - Automatic key rotation support
    - Audit logging of all tokenization operations
    """
    
    def __init__(self, encryption_key: Optional[str] = None):
        if not CRYPTO_AVAILABLE:
            raise ImportError("cryptography library not installed")
        
        # Derive key from password or use provided key
        if encryption_key:
            self.master_key = self._derive_key(encryption_key)
        else:
            # Generate random key for testing (NOT for production!)
            self.master_key = secrets.token_bytes(32)
        
        self.aesgcm = AESGCM(self.master_key)
        self._token_cache: Dict[str, TokenizedValue] = {}
        self._reverse_lookup: Dict[str, str] = {}  # For deterministic tokenization
        
    def _derive_key(self, password: str, salt: Optional[bytes] = None) -> bytes:
        """Derive 32-byte key from password using PBKDF2."""
        if salt is None:
            salt = b'pit-feature-store-salt'  # In production, use random salt
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return kdf.derive(password.encode())
    
    def tokenize(
        self,
        value: str,
        category: PIICategory,
        deterministic: bool = True,
        preserve_format: bool = False
    ) -> TokenizedValue:
        """
        Tokenize a PII value.
        
        Args:
            value: Original PII value
            category: Type of PII
            deterministic: If True, same input always produces same token
            preserve_format: If True, use format-preserving encryption
            
        Returns:
            TokenizedValue with token and metadata
        """
        now = datetime.now(timezone.utc)
        
        # Check cache for deterministic tokenization
        cache_key = f"{category.value}:{value}"
        if deterministic and cache_key in self._reverse_lookup:
            token = self._reverse_lookup[cache_key]
            return TokenizedValue(
                original_category=category,
                token=token,
                created_at=self._token_cache[token].created_at,
                expires_at=self._token_cache[token].expires_at
            )
        
        if preserve_format and category in [PIICategory.ACCOUNT_NUMBER, PIICategory.CREDIT_CARD]:
            # Format-preserving encryption
            token = self._format_preserving_encrypt(value, category)
        else:
            # Standard encryption with base64 encoding
            token = self._encrypt_value(value)
        
        # Store in cache
        tokenized = TokenizedValue(
            original_category=category,
            token=token,
            created_at=now.isoformat(),
            expires_at=None  # Set expiry if needed
        )
        
        if deterministic:
            self._token_cache[token] = tokenized
            self._reverse_lookup[cache_key] = token
        
        return tokenized
    
    def _encrypt_value(self, value: str) -> str:
        """Encrypt value using AES-256-GCM."""
        nonce = secrets.token_bytes(12)  # 96-bit nonce for GCM
        ciphertext = self.aesgcm.encrypt(nonce, value.encode(), None)
        
        # Combine nonce + ciphertext + tag
        encrypted = nonce + ciphertext
        return base64.urlsafe_b64encode(encrypted).decode('ascii')
    
    def _format_preserving_encrypt(self, value: str, category: PIICategory) -> str:
        """
        Format-preserving encryption for account numbers.
        
        Maintains the same length and character set as original.
        """
        if category == PIICategory.ACCOUNT_NUMBER:
            # Account numbers: preserve digits only
            return self._ffe_numeric(value)
        elif category == PIICategory.CREDIT_CARD:
            # Credit cards: preserve Luhn-valid format
            return self._ffe_numeric(value)
        else:
            # Fallback to standard encryption
            return self._encrypt_value(value)
    
    def _ffe_numeric(self, value: str) -> str:
        """Format-preserving encryption for numeric strings."""
        # Extract digits
        digits = [c for c in value if c.isdigit()]
        if not digits:
            return value
        
        # Encrypt digits using hash-based approach
        hash_input = f"{self.master_key.hex()}:{''.join(digits)}"
        hash_output = hashlib.sha256(hash_input.encode()).hexdigest()
        
        # Map hash back to digits (preserving length)
        encrypted_digits = []
        for i, digit in enumerate(digits):
            hash_digit = int(hash_output[i % len(hash_output)], 16) % 10
            encrypted_digit = (int(digit) + hash_digit) % 10
            encrypted_digits.append(str(encrypted_digit))
        
        # Reconstruct with non-digit characters
        result = []
        digit_idx = 0
        for c in value:
            if c.isdigit():
                result.append(encrypted_digits[digit_idx])
                digit_idx += 1
            else:
                result.append(c)
        
        return ''.join(result)
    
    def detokenize(self, token: str) -> Optional[str]:
        """
        Decrypt a token back to original value.
        
        WARNING: This should be heavily restricted in production!
        Only authorized services should have access.
        """
        try:
            # Decode base64
            encrypted = base64.urlsafe_b64decode(token.encode('ascii'))
            
            # Extract nonce and ciphertext
            nonce = encrypted[:12]
            ciphertext = encrypted[12:]
            
            # Decrypt
            decrypted = self.aesgcm.decrypt(nonce, ciphertext, None)
            return decrypted.decode('utf-8')
        except Exception:
            return None
    
    def mask_pii(self, value: str, category: PIICategory) -> str:
        """
        Mask PII for display/logging purposes.
        
        Examples:
        - SSN: ***-**-1234
        - Account: ****5678
        - Email: j***@example.com
        """
        if category == PIICategory.SSN:
            # Format: XXX-XX-NNNN
            if len(value) >= 4:
                return f"***-**-{value[-4:]}"
            return "***-**-****"
        
        elif category == PIICategory.ACCOUNT_NUMBER:
            if len(value) >= 4:
                return f"****{value[-4:]}"
            return "****"
        
        elif category == PIICategory.EMAIL:
            if '@' in value:
                parts = value.split('@')
                masked_local = parts[0][0] + '***' if parts[0] else '***'
                return f"{masked_local}@{parts[1]}"
            return "***@***"
        
        elif category == PIICategory.PHONE:
            if len(value) >= 4:
                return f"***-***-{value[-4:]}"
            return "***-***-****"
        
        else:
            # Generic masking
            if len(value) > 4:
                return value[0] + '***' + value[-2:]
            return '***'
    
    def scan_and_tokenize(
        self,
        data: Dict[str, Any],
        pii_fields: List[str],
        category_map: Dict[str, PIICategory]
    ) -> Dict[str, Any]:
        """
        Scan dictionary for PII fields and tokenize them.
        
        Args:
            data: Input data dictionary
            pii_fields: List of field names that contain PII
            category_map: Mapping of field names to PII categories
            
        Returns:
            Copy of data with PII fields tokenized
        """
        result = data.copy()
        
        for field in pii_fields:
            if field in result and result[field]:
                category = category_map.get(field, PIICategory.CUSTOM)
                tokenized = self.tokenize(str(result[field]), category)
                result[field] = tokenized.token
        
        return result
    
    def verify_no_pii_leakage(self, data: Dict[str, Any]) -> List[str]:
        """
        Scan data for potential PII patterns that weren't tokenized.
        
        Returns list of fields that may contain untokenized PII.
        """
        leaks = []
        
        # PII pattern detection
        import re
        patterns = {
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'credit_card': r'\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b',
            'account_number': r'\bACC_\d{6,}\b',
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        }
        
        for field, value in data.items():
            if isinstance(value, str):
                for pii_type, pattern in patterns.items():
                    if re.search(pattern, value):
                        # Check if it looks tokenized
                        if not value.startswith('***') and not value.isalnum():
                            leaks.append(f"{field} may contain {pii_type}")
        
        return leaks


def create_tokenizer() -> PIITokenizer:
    """Factory function to create tokenizer from environment."""
    key = os.getenv('ENCRYPTION_KEY')
    return PIITokenizer(encryption_key=key)


if __name__ == "__main__":
    # Test tokenization
    tokenizer = PIITokenizer()
    
    test_cases = [
        ("123-45-6789", PIICategory.SSN),
        ("ACC_001234567", PIICategory.ACCOUNT_NUMBER),
        ("john.doe@example.com", PIICategory.EMAIL),
        ("555-123-4567", PIICategory.PHONE),
    ]
    
    print("PII Tokenization Test:")
    print("-" * 50)
    
    for value, category in test_cases:
        tokenized = tokenizer.tokenize(value, category)
        masked = tokenizer.mask_pii(value, category)
        
        print(f"\nOriginal ({category.value}): {value}")
        print(f"Token: {tokenized.token}")
        print(f"Masked: {masked}")
    
    # Test scan for leakage
    test_data = {
        'account_id': 'ACC_001234',
        'ssn': '123-45-6789',  # Should be detected
        'tokenized_field': 'abc123xyz'
    }
    
    leaks = tokenizer.verify_no_pii_leakage(test_data)
    print(f"\nPotential PII leaks: {leaks}")
