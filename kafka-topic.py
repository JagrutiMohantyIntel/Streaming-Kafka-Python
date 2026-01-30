
# PreReq: pip install kafka-python
from kafka import KafkaAdminClient
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError
import os
from dotenv import load_dotenv
import ssl

# Load environment variables
load_dotenv()

bootstrap_servers = "10-63-84-38.dbaas.intel.com:9094,10-63-84-39.dbaas.intel.com:9094"
secProtocol = 'SASL_SSL'
saslMech = 'SCRAM-SHA-512'
username = os.getenv('KAFKA_USERNAME')
password = os.getenv('KAFKA_PASSWORD')

context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
context.verify_mode = ssl.CERT_REQUIRED
context.load_default_certs()

def create_topic():
    try:
        print("About to Connect to {}".format(bootstrap_servers))
        adminClient = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            security_protocol=secProtocol,
            sasl_mechanism=saslMech,
            sasl_plain_username=username,
            sasl_plain_password=password,
            ssl_context=context
        )
        print("Successfully connected to Kafka Instance {}".format(adminClient))
        
        # Commands to create topic
        cust_topic = "topicA"
        topic_list = NewTopic(name=cust_topic, num_partitions=2, replication_factor=2)
        
        try:
            adminClient.create_topics(new_topics=[topic_list], validate_only=False)
            print("Successfully created topic {}".format(cust_topic))
        except TopicAlreadyExistsError:
            print("Topic '{}' already exists!".format(cust_topic))
        
        # List existing topics to verify
        metadata = adminClient.list_topics()
        print("Available topics:", list(metadata))
        
    except Exception as ex:
        print("Failed with Error: {}".format(ex))
    finally:
        try:
            adminClient.close()
        except:
            pass

if __name__ == "__main__":
    create_topic()