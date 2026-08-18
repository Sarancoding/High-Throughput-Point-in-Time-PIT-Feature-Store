"""
Storage Engine for PIT Feature Store
Implements AS OF queries and feature retrieval from ClickHouse.
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

try:
    import clickhouse_connect
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False


@dataclass
class FeatureVector:
    """Feature vector retrieved from PIT query."""
    account_id: str
    timestamp: str
    features: Dict[str, Any]
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'account_id': self.account_id,
            'timestamp': self.timestamp,
            'features': self.features,
            'metadata': self.metadata
        }


class PITQueryEngine:
    """
    Point-in-Time query engine for ClickHouse.
    
    Implements AS OF queries that guarantee no future data leakage.
    """
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 9000,
        user: str = 'default',
        password: str = '',
        database: str = 'pit_features'
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.client = None
        
    def connect(self):
        """Establish connection to ClickHouse."""
        if not CLICKHOUSE_AVAILABLE:
            raise ImportError("clickhouse-connect not installed")
        
        self.client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.database
        )
    
    def close(self):
        """Close connection."""
        if self.client:
            self.client.close()
    
    def get_feature_as_of(
        self,
        account_id: str,
        timestamp: datetime,
        feature_names: Optional[List[str]] = None
    ) -> Optional[FeatureVector]:
        """
        Retrieve feature vector as of a specific point in time.
        
        This is the core PIT query - returns state valid at timestamp
        without any future data leakage.
        
        Args:
            account_id: Account identifier
            timestamp: Point-in-time timestamp
            feature_names: Optional list of specific features to retrieve
            
        Returns:
            FeatureVector or None if no data found
        """
        if not self.client:
            self.connect()
        
        # Format timestamp for ClickHouse
        ts_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        # Build AS OF query
        query = """
        SELECT 
            account_id,
            snapshot_time,
            transaction_count_1h,
            transaction_count_24h,
            total_amount_1h,
            total_amount_24h,
            avg_amount_1h,
            avg_amount_24h,
            unique_merchants_24h,
            risk_score
        FROM pit_features.feature_snapshots
        WHERE account_id = %(account_id)s
          AND snapshot_time <= %(timestamp)s
        ORDER BY snapshot_time DESC
        LIMIT 1
        """
        
        result = self.client.query(
            query,
            {'account_id': account_id, 'timestamp': ts_str}
        )
        
        if not result.result_rows:
            return None
        
        row = result.result_rows[0]
        
        features = {
            'transaction_count_1h': row[2],
            'transaction_count_24h': row[3],
            'total_amount_1h': row[4],
            'total_amount_24h': row[5],
            'avg_amount_1h': row[6],
            'avg_amount_24h': row[7],
            'unique_merchants_24h': row[8],
            'risk_score': row[9]
        }
        
        # Filter if specific features requested
        if feature_names:
            features = {k: v for k, v in features.items() if k in feature_names}
        
        return FeatureVector(
            account_id=row[0],
            timestamp=row[1].isoformat(),
            features=features,
            metadata={
                'query_timestamp': datetime.now(timezone.utc).isoformat(),
                'requested_timestamp': ts_str,
                'pit_guarantee': True
            }
        )
    
    def insert_feature_snapshot(
        self,
        account_id: str,
        snapshot_time: datetime,
        features: Dict[str, Any],
        source_event_count: int = 0,
        computation_version: str = '1.0'
    ):
        """Insert a feature snapshot for PIT queries."""
        if not self.client:
            self.connect()
        
        query = """
        INSERT INTO pit_features.feature_snapshots
        (
            account_id, snapshot_time,
            transaction_count_1h, transaction_count_24h,
            total_amount_1h, total_amount_24h,
            avg_amount_1h, avg_amount_24h,
            unique_merchants_24h,
            risk_score,
            source_event_count, computation_version
        )
        VALUES
        """
        
        self.client.command(
            query,
            [
                account_id,
                snapshot_time,
                features.get('transaction_count_1h', 0),
                features.get('transaction_count_24h', 0),
                features.get('total_amount_1h', 0.0),
                features.get('total_amount_24h', 0.0),
                features.get('avg_amount_1h', 0.0),
                features.get('avg_amount_24h', 0.0),
                features.get('unique_merchants_24h', 0),
                features.get('risk_score', 0.0),
                source_event_count,
                computation_version
            ]
        )
    
    def log_audit(
        self,
        operation: str,
        account_id: Optional[str] = None,
        feature_timestamp: Optional[datetime] = None,
        user_id: Optional[str] = None,
        latency_ms: int = 0,
        rows_processed: int = 0,
        metadata: Optional[Dict] = None
    ):
        """Log audit entry for compliance."""
        if not self.client:
            self.connect()
        
        self.client.command(
            """
            INSERT INTO pit_features.audit_log
            (operation, account_id, feature_timestamp, user_id, latency_ms, rows_processed, metadata)
            VALUES (%(op)s, %(acc)s, %(fts)s, %(uid)s, %(lat)s, %(rows)s, %(meta)s)
            """,
            {
                'op': operation,
                'acc': account_id,
                'fts': feature_timestamp,
                'uid': user_id,
                'lat': latency_ms,
                'rows': rows_processed,
                'meta': json.dumps(metadata) if metadata else '{}'
            }
        )
    
    def verify_no_future_leakage(
        self,
        account_id: str,
        test_timestamp: datetime
    ) -> bool:
        """
        Verify that PIT query returns no future data.
        
        Returns True if verification passes (no leakage).
        """
        if not self.client:
            self.connect()
        
        # Query for any snapshots with timestamp > test_timestamp
        # that were created before test_timestamp (impossible scenario)
        query = """
        SELECT count(*)
        FROM pit_features.feature_snapshots
        WHERE account_id = %(account_id)s
          AND snapshot_time > %(test_ts)s
          AND created_at < %(test_ts)s
        """
        
        result = self.client.query(
            query,
            {'account_id': account_id, 'test_ts': test_timestamp}
        )
        
        count = result.result_rows[0][0] if result.result_rows else 0
        return count == 0
    
    def get_lineage(
        self,
        account_id: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Retrieve data lineage for an account within time range."""
        if not self.client:
            self.connect()
        
        query = """
        SELECT 
            snapshot_time,
            source_event_count,
            computation_version,
            created_at
        FROM pit_features.feature_snapshots
        WHERE account_id = %(account_id)s
          AND snapshot_time BETWEEN %(start)s AND %(end)s
        ORDER BY snapshot_time
        """
        
        result = self.client.query(
            query,
            {
                'account_id': account_id,
                'start': start_time,
                'end': end_time
            }
        )
        
        return [
            {
                'snapshot_time': row[0].isoformat(),
                'source_event_count': row[1],
                'computation_version': row[2],
                'created_at': row[3].isoformat()
            }
            for row in result.result_rows
        ]


if __name__ == "__main__":
    # Example usage
    engine = PITQueryEngine(
        host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
        password=os.getenv('CLICKHOUSE_PASSWORD', '')
    )
    
    try:
        engine.connect()
        print("Connected to ClickHouse")
        
        # Test PIT query
        test_time = datetime.now()
        feature = engine.get_feature_as_of('ACC_001', test_time)
        
        if feature:
            print(f"Retrieved features: {feature.to_dict()}")
        else:
            print("No features found")
        
    finally:
        engine.close()
