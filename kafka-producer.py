from kafka import KafkaProducer
from kafka.errors import KafkaError
import ssl
import json
import time
from datetime import datetime
import random
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Your configuration
bootstrap_servers = "10-63-84-38.dbaas.intel.com:9094,10-63-84-39.dbaas.intel.com:9094"
secProtocol = 'SASL_SSL'
saslMech = 'SCRAM-SHA-512'
username = os.getenv('KAFKA_USERNAME')
password = os.getenv('KAFKA_PASSWORD')
topic_name = "topicA"

# SSL Context
context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
context.verify_mode = ssl.CERT_REQUIRED
context.load_default_certs()

class DeltaDataGenerator:
    def __init__(self):
        # Simulate a database state
        self.current_state = {
            'record_1': {'id': 1, 'value': 1, 'status': 'active', 'last_updated': '2024-01-01'},
            'record_2': {'id': 2, 'value': 2, 'status': 'active', 'last_updated': '2024-01-01'},
            'record_3': {'id': 3, 'value': 3, 'status': 'active', 'last_updated': '2024-01-01'},
            'record_4': {'id': 3, 'value': 3, 'status': 'active', 'last_updated': '2024-01-01'},
        }
    
    def generate_delta_changes(self):
        """Generate realistic delta changes"""
        changes = []
        
        # Randomly select records to modify
        records_to_modify = random.sample(list(self.current_state.keys()), 
                                        random.randint(1, len(self.current_state)))
        
        for record_key in records_to_modify:
            change_type = random.choice(['update_value', 'update_status', 'add_field', 'update_multiple'])
            
            if change_type == 'update_value':
                # Update the value field
                old_value = self.current_state[record_key]['value']
                new_value = old_value + random.randint(1, 10)
                self.current_state[record_key]['value'] = new_value
                self.current_state[record_key]['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
            elif change_type == 'update_status':
                # Update status
                current_status = self.current_state[record_key]['status']
                new_status = 'inactive' if current_status == 'active' else 'active'
                self.current_state[record_key]['status'] = new_status
                self.current_state[record_key]['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
            elif change_type == 'add_field':
                # Add a new field
                self.current_state[record_key]['priority'] = random.choice(['high', 'medium', 'low'])
                self.current_state[record_key]['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
            elif change_type == 'update_multiple':
                # Update multiple fields
                self.current_state[record_key]['value'] += random.randint(1, 5)
                self.current_state[record_key]['category'] = random.choice(['A', 'B', 'C'])
                self.current_state[record_key]['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            changes.append({
                'key': record_key,
                'data': self.current_state[record_key].copy(),
                'change_type': change_type
            })
        
        return changes

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol=secProtocol,
            sasl_mechanism=saslMech,
            sasl_plain_username=username,
            sasl_plain_password=password,
            ssl_context=context,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
            compression_type='gzip',
            batch_size=16384,
            linger_ms=10,
            request_timeout_ms=40000,
            delivery_timeout_ms=120000
        )
        print("✓ Producer connected successfully")
        return producer
    except Exception as ex:
        print(f"✗ Failed to create producer: {ex}")
        return None

def send_delta_records():
    producer = create_producer()
    if not producer:
        return
    
    delta_generator = DeltaDataGenerator()
    
    print("🚀 Starting delta record producer...")
    print("   Sending incremental changes instead of full arrays")
    print("=" * 70)
    
    try:
        # Send initial state first
        print("📤 Sending initial state...")
        for key, data in delta_generator.current_state.items():
            message = {
                'record_type': 'initial_load',
                'timestamp': datetime.now().isoformat(),
                **data
            }
            
            future = producer.send(topic_name, key=key, value=message)
            record_metadata = future.get(timeout=10)
            print(f"   ✓ Initial record '{key}' sent to partition {record_metadata.partition}")
        
        print("\n" + "="*70)
        
        # Send delta changes over time
        for round_num in range(1, 6):  # 5 rounds of changes
            print(f"\n🔄 Round {round_num}: Generating delta changes...")
            
            changes = delta_generator.generate_delta_changes()
            
            for change in changes:
                message = {
                    'record_type': 'delta_change',
                    'change_round': round_num,
                    'change_type': change['change_type'],
                    'timestamp': datetime.now().isoformat(),
                    **change['data']
                }
                
                print(f"📤 Sending delta for '{change['key']}' (change: {change['change_type']})")
                print(f"   Data: {json.dumps(change['data'], indent=8)}")
                
                future = producer.send(
                    topic_name, 
                    key=change['key'], 
                    value=message
                )
                
                try:
                    record_metadata = future.get(timeout=10)
                    print(f"   ✓ Delta sent successfully!")
                    print(f"     Topic: {record_metadata.topic}")
                    print(f"     Partition: {record_metadata.partition}")
                    print(f"     Offset: {record_metadata.offset}")
                except KafkaError as e:
                    print(f"   ✗ Failed to send delta: {e}")
                
                print("-" * 50)
            
            time.sleep(3)  # Wait between rounds
            
    except Exception as e:
        print(f"❌ Error in send_delta_records: {e}")
    finally:
        producer.flush()
        producer.close()
        print("✅ Producer closed")

if __name__ == "__main__":
    send_delta_records()