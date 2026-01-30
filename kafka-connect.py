# Sample python program to connect to Kafka instance
# PreReq: pip install kafka-python
#completed step https://wiki.ith.intel.com/spaces/DBaas/pages/4233384568/Connecting+to+Kafka+Using+Python+kafka-python
from kafka import KafkaAdminClient
import ssl
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get credentials from environment
#username = os.getenv('KAFKA_USERNAME')
#password = os.getenv('KAFKA_PASSWORD')



# Establish Connection to Kafka
def establish_connection(bootstrap_servers):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_default_certs()
    try:
        print("About to Connect to {}".format(bootstrap_servers))
        adminClint = KafkaAdminClient(bootstrap_servers=bootstrap_servers,security_protocol=secProtocol,sasl_mechanism=saslMech,sasl_plain_username=username,sasl_plain_password=password)
        print("Successfully connected to Kafka Instance {}".format(adminClint))
    except Exception as ex:
        print("Failed to connect {}".format(ex))

if __name__ == '__main__':
    bootstrap_servers = "10-63-84-38.dbaas.intel.com:9094,10-63-84-39.dbaas.intel.com:9094"
    secProtocol = 'SASL_SSL' 
    saslMech = 'SCRAM-SHA-512' 
    username = os.getenv('KAFKA_USERNAME')
    password = os.getenv('KAFKA_PASSWORD')
    establish_connection(bootstrap_servers)