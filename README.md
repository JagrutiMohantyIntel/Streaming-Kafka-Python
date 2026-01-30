# Streaming-Kafka-Python

# Kafka Python Delta Consumer/Producer

This project demonstrates Kafka consumer and producer implementation for processing delta records using Python.

## Features

- **Delta Consumer**: Consumes delta records from Kafka topic with SASL_SSL authentication
- **Delta Producer**: Produces sample delta records to Kafka topic
- **SSL/SASL Authentication**: Secure connection using SCRAM-SHA-512
- **JSON Serialization**: Automatic JSON serialization/deserialization

## Prerequisites

- Python 3.7+
- kafka-python library
- python-dotenv library
- Access to Kafka cluster with SASL_SSL

## Installation

1. Clone the repository:
```bash
git clone https://github.com/JagrutiMohantyIntel/Streaming-Kafka-Python.git



Run Locally on Windows

pip install -r requirements.txt

1. Download the Executable: Get the executable from the Redpanda Console v2.7.1 (ensure to download "amd" if you are going run Redpand on windows).

2. Run the Executable: Open a Windows Terminal and navigate to the directory where the executable is located. Then run the following command, I have added into my same folder:


PS C:\Users\acharbha\Downloads\redpanda_console_2.7.1_windows_amd64> .\redpanda-console.exe --config.filepath "config.yaml" --kafka.sasl.password "password"

