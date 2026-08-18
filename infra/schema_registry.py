"""
Schema Registry for CDC Events
Manages Avro/JSON schema evolution and validation.
"""

import json
from typing import Dict, Any, Optional
from datetime import datetime
import hashlib


class SchemaRegistry:
    """Simple schema registry with versioning and compatibility checking."""
    
    def __init__(self):
        self._schemas: Dict[str, Dict[int, Dict[str, Any]]] = {}
        self._current_version: Dict[str, int] = {}
        
    def register_schema(
        self, 
        subject: str, 
        schema: Dict[str, Any],
        compatibility: str = "BACKWARD"
    ) -> int:
        """
        Register a new schema version.
        
        Args:
            subject: Schema subject (e.g., 'cdc_transactions-value')
            schema: Schema definition
            compatibility: Compatibility mode (BACKWARD, FORWARD, FULL)
            
        Returns:
            Version number
        """
        if subject not in self._schemas:
            self._schemas[subject] = {}
            self._current_version[subject] = 0
        
        new_version = self._current_version[subject] + 1
        
        # Validate compatibility with previous version
        if new_version > 1:
            prev_schema = self._schemas[subject][new_version - 1]
            if not self._check_compatibility(prev_schema, schema, compatibility):
                raise ValueError(
                    f"Schema incompatible with previous version. "
                    f"Mode: {compatibility}"
                )
        
        self._schemas[subject][new_version] = {
            'schema': schema,
            'registered_at': datetime.utcnow().isoformat(),
            'compatibility': compatibility
        }
        self._current_version[subject] = new_version
        
        return new_version
    
    def _check_compatibility(
        self, 
        old_schema: Dict[str, Any], 
        new_schema: Dict[str, Any],
        mode: str
    ) -> bool:
        """Check schema compatibility based on mode."""
        old_fields = {f['name']: f for f in old_schema.get('fields', [])}
        new_fields = {f['name']: f for f in new_schema.get('fields', [])}
        
        if mode == "BACKWARD":
            # New schema can read old data: new required fields must have defaults
            for name, field in old_fields.items():
                if name in new_fields:
                    # Check type compatibility
                    if not self._types_compatible(field['type'], new_fields[name]['type']):
                        return False
                elif field.get('default') is None and not field.get('nullable', False):
                    # Old required field removed from new schema
                    return False
                    
        elif mode == "FORWARD":
            # Old schema can read new data: old required fields must have defaults in new
            for name, field in new_fields.items():
                if name in old_fields:
                    if not self._types_compatible(old_fields[name]['type'], field['type']):
                        return False
                elif field.get('default') is None and not field.get('nullable', False):
                    # New required field added without default
                    return False
                    
        elif mode == "FULL":
            # Both backward and forward compatible
            return (self._check_compatibility(old_schema, new_schema, "BACKWARD") and
                    self._check_compatibility(old_schema, new_schema, "FORWARD"))
        
        return True
    
    def _types_compatible(self, old_type: str, new_type: str) -> bool:
        """Check if two types are compatible."""
        # Simple type compatibility check
        type_hierarchy = {
            'int': ['int', 'long', 'float', 'double'],
            'long': ['long', 'float', 'double'],
            'float': ['float', 'double'],
            'double': ['double'],
            'string': ['string', 'bytes'],
            'bytes': ['bytes'],
        }
        
        if old_type == new_type:
            return True
            
        compatible_types = type_hierarchy.get(old_type, [old_type])
        return new_type in compatible_types
    
    def get_schema(self, subject: str, version: Optional[int] = None) -> Dict[str, Any]:
        """Get schema by subject and optional version."""
        if subject not in self._schemas:
            raise KeyError(f"Subject not found: {subject}")
        
        if version is None:
            version = self._current_version[subject]
        
        if version not in self._schemas[subject]:
            raise KeyError(f"Version {version} not found for subject {subject}")
        
        return self._schemas[subject][version]['schema']
    
    def get_current_version(self, subject: str) -> int:
        """Get current schema version for subject."""
        if subject not in self._current_version:
            raise KeyError(f"Subject not found: {subject}")
        return self._current_version[subject]
    
    def validate_event(self, subject: str, event: Dict[str, Any]) -> bool:
        """Validate an event against the current schema."""
        schema = self.get_schema(subject)
        return self._validate_against_schema(event, schema)
    
    def _validate_against_schema(
        self, 
        event: Dict[str, Any], 
        schema: Dict[str, Any]
    ) -> bool:
        """Validate event data against schema definition."""
        required_fields = schema.get('required', [])
        fields = {f['name']: f for f in schema.get('fields', [])}
        
        # Check required fields
        for field_name in required_fields:
            if field_name not in event:
                return False
        
        # Check field types
        for field_name, value in event.items():
            if field_name in fields:
                field_def = fields[field_name]
                if not self._validate_type(value, field_def['type']):
                    return False
        
        return True
    
    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """Validate value against expected type."""
        type_checks = {
            'string': lambda v: isinstance(v, str),
            'int': lambda v: isinstance(v, int),
            'long': lambda v: isinstance(v, int),
            'float': lambda v: isinstance(v, (int, float)),
            'double': lambda v: isinstance(v, (int, float)),
            'boolean': lambda v: isinstance(v, bool),
            'bytes': lambda v: isinstance(v, (bytes, bytearray)),
            'array': lambda v: isinstance(v, list),
            'map': lambda v: isinstance(v, dict),
            'record': lambda v: isinstance(v, dict),
        }
        
        checker = type_checks.get(expected_type, lambda v: True)
        return checker(value)


# Pre-register CDC transaction schema
CDC_TRANSACTION_SCHEMA = {
    "type": "record",
    "name": "CDCTransaction",
    "namespace": "com.pit.features",
    "fields": [
        {"name": "transaction_id", "type": "string"},
        {"name": "account_id", "type": "string"},
        {"name": "event_time", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "processing_time", "type": ["null", "string"], "default": None},
        {"name": "_dedup_key", "type": ["null", "string"], "default": None},
        {
            "name": "data",
            "type": {
                "type": "record",
                "name": "TransactionData",
                "fields": [
                    {"name": "amount", "type": "double"},
                    {"name": "currency", "type": "string"},
                    {"name": "merchant_id", "type": ["null", "string"], "default": None},
                    {"name": "category", "type": ["null", "string"], "default": None}
                ]
            }
        }
    ],
    "required": ["transaction_id", "account_id", "event_time", "event_type", "data"]
}


if __name__ == "__main__":
    registry = SchemaRegistry()
    
    # Register initial schema
    version = registry.register_schema("cdc_transactions-value", CDC_TRANSACTION_SCHEMA)
    print(f"Registered schema version: {version}")
    
    # Test validation
    test_event = {
        "transaction_id": "tx_123",
        "account_id": "acc_456",
        "event_time": "2024-01-01T00:00:00Z",
        "event_type": "TRANSACTION",
        "data": {
            "amount": 100.50,
            "currency": "USD"
        }
    }
    
    is_valid = registry.validate_event("cdc_transactions-value", test_event)
    print(f"Event valid: {is_valid}")
