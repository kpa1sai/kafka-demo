import os
import time
import sys
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, KafkaError

# Config
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
TOPIC_NAME = 'demo-topic'
ACKS = os.environ.get('ACKS', 'all') # 'all', '1', or '0'

print(f"Starting producer with ACKS={ACKS}...")

# 1. Wait for Kafka to be available and create the topic
admin = None
while not admin:
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id='producer-setup')
    except NoBrokersAvailable:
        print("Waiting for Kafka brokers to be available...")
        time.sleep(2)
    except Exception as e:
        print(f"Error connecting: {e}")
        time.sleep(2)

try:
    existing_topics = admin.list_topics()
    if TOPIC_NAME not in existing_topics:
        print(f"Creating topic '{TOPIC_NAME}' with 3 replicas and min.insync.replicas=2...")
        topic = NewTopic(
            name=TOPIC_NAME,
            num_partitions=1,
            replication_factor=3,
            topic_configs={'min.insync.replicas': '2'}
        )
        admin.create_topics([topic])
        print("Topic created successfully.")
    else:
        print(f"Topic '{TOPIC_NAME}' already exists.")
except Exception as e:
    print(f"Error checking/creating topic: {e}")
finally:
    admin.close()

# 2. Initialize Producer
producer = None
while not producer:
    try:
        # Translate acks string to numeric/expected values
        # kafka-python takes acks=0, acks=1, acks='all' (or 1, 0, -1)
        # We can pass integer 0, 1, or string 'all'
        acks_val = ACKS
        if acks_val in ('0', '1'):
            acks_val = int(acks_val)
        
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            acks=acks_val,
            request_timeout_ms=5000,
            retries=5, # Allow retries for seamless leader transition
            retry_backoff_ms=1000, # Wait 1s between retries
            linger_ms=0
        )
    except NoBrokersAvailable:
        print("Producer waiting for brokers...")
        time.sleep(2)

print("Producer initialized. Starting stream...")

seq = 0
while True:
    seq += 1
    msg_val = f"Message #{seq} - timestamp: {time.time()}".encode('utf-8')
    
    try:
        # Send asynchronously, then wait for result
        future = producer.send(TOPIC_NAME, key=f"key-{seq}".encode('utf-8'), value=msg_val)
        record_metadata = future.get(timeout=5.0)
        print(f"🟢 [Success] Sent msg #{seq:04d} -> partition {record_metadata.partition} @ offset {record_metadata.offset}")
    except KafkaError as ke:
        print(f"🔴 [FAIL] Msg #{seq:04d} failed: {ke.__class__.__name__} - {str(ke)}")
    except Exception as e:
        print(f"🔴 [FAIL] Msg #{seq:04d} failed: {str(e)}")
        
    time.sleep(1.0)
