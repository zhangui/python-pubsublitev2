import os
from unittest.mock import MagicMock, patch
from google.cloud.pubsublite_v1 import CursorServiceClient
from google.cloud.pubsublite_v1.types import cursor, common

if __name__ == "__main__":
    # Simple manual verification if confluent_kafka is installed
    try:
        import confluent_kafka
        print("confluent_kafka is installed.")
        
        kafka_config = {'bootstrap.servers': 'localhost:9092'}
        try:
            mock_creds = MagicMock()
            client = CursorServiceClient(
                transport="kafka", 
                kafka_config=kafka_config,
                credentials=mock_creds
            )
            print("Successfully created CursorServiceClient with transport='kafka'")
            print(f"Transport type: {type(client.transport)}")
            print(f"Transport kind: {client.transport.kind}")
            
            # Verify methods exist
            assert hasattr(client.transport, 'commit_cursor')
            assert hasattr(client.transport, 'list_partition_cursors')
            print("Verified transport methods exist.")
            
        except Exception as e:
            print(f"Failed to create client: {e}")
            import traceback
            traceback.print_exc()
            
    except ImportError:
        print("confluent_kafka not installed. Skipping verification.")
