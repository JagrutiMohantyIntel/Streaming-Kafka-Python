from kafka import KafkaConsumer
from kafka.errors import KafkaError
import ssl
import json
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
delta_topic = "topicA_delta"

# SSL Context
context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
context.verify_mode = ssl.CERT_REQUIRED
context.load_default_certs()

def create_delta_consumer():
    try:
        consumer = KafkaConsumer(
            delta_topic,
            bootstrap_servers=bootstrap_servers,
            security_protocol=secProtocol,
            sasl_mechanism=saslMech,
            sasl_plain_username=username,
            sasl_plain_password=password,
            ssl_context=context,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='delta-consumer-group',
            consumer_timeout_ms=30000
        )
        print("✓ Delta consumer connected successfully")
        return consumer
    except Exception as ex:
        print(f"✗ Failed to create delta consumer: {ex}")
        return None

def consume_delta_records():
    consumer = create_delta_consumer()
    if not consumer:
        return
    
    print(f"🎯 Consuming delta records from '{delta_topic}'...")
    print("   Press Ctrl+C to stop")
    print("=" * 80)
    
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            delta_record = message.value
            
            print(f"\n🔄 Delta Record #{message_count}:")
            print(f"   Key: {message.key}")
            print(f"   Operation: {delta_record.get('operation', 'N/A')}")
            print(f"   Timestamp: {delta_record.get('timestamp', 'N/A')}")
            
            if 'changes' in delta_record and delta_record['changes']:
                print(f"   Changes:")
                for field, change_info in delta_record['changes'].items():
                    if isinstance(change_info, dict):
                        print(f"     {field}: {change_info['operation']} "
                              f"({change_info['old_value']} → {change_info['new_value']})")
                    else:
                        print(f"     {field}: {change_info}")
            
            print(f"   Partition: {message.partition}, Offset: {message.offset}")
            print("-" * 80)
            
    except KeyboardInterrupt:
        print(f"\n🛑 Stopping delta consumer... Processed {message_count} delta records")
    except Exception as e:
        print(f"❌ Error in delta consumer: {e}")
    finally:
        consumer.close()
        print("✅ Delta consumer closed")

if __name__ == "__main__":
    consume_delta_records()