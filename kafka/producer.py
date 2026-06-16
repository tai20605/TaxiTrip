import csv
import json
import os
import signal
import sys
import time
import uuid
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

def create_topics_if_not_exists(bootstrap_servers, topic_name, dlq_topic_name):
    """Create required Kafka topics if they do not exist."""
    print(f"[Producer] Verifying Kafka topics on {bootstrap_servers}...", flush=True)
    try:
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    except Exception as e:
        print(f"[Producer] Failed to create AdminClient: {e}", file=sys.stderr, flush=True)
        return

    # Fetch metadata to check if topics exist
    try:
        metadata = admin_client.list_topics(timeout=10.0)
    except Exception as e:
        print(f"[Producer] Failed to list Kafka topics: {e}. Kafka might not be ready.", file=sys.stderr, flush=True)
        return

    existing_topics = metadata.topics.keys()
    new_topics = []

    # 1. Main Topic
    if topic_name not in existing_topics:
        print(f"[Producer] Topic '{topic_name}' not found. Adding to creation queue...", flush=True)
        new_topics.append(NewTopic(
            topic_name,
            num_partitions=3,
            replication_factor=1,
            config={
                'retention.ms': '604800000',
                'retention.bytes': '2147483648',
                'compression.type': 'lz4',
                'max.message.bytes': '10485760'
            }
        ))
    else:
        print(f"[Producer] Topic '{topic_name}' already exists.", flush=True)

    # 2. DLQ Topic
    if dlq_topic_name not in existing_topics:
        print(f"[Producer] Topic '{dlq_topic_name}' not found. Adding to creation queue...", flush=True)
        new_topics.append(NewTopic(
            dlq_topic_name,
            num_partitions=1,
            replication_factor=1,
            config={
                'retention.ms': '2592000000'
            }
        ))
    else:
        print(f"[Producer] Topic '{dlq_topic_name}' already exists.", flush=True)

    if new_topics:
        fs = admin_client.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()  # Block until topic is created
                print(f"[Producer] Topic '{topic}' created successfully.", flush=True)
            except Exception as e:
                print(f"[Producer] Failed to create topic '{topic}': {e}", file=sys.stderr, flush=True)


# Global flag for graceful shutdown
running = True

def signal_handler(signum, frame):
    global running
    print(f"\n[Producer] Received shutdown signal ({signum}). Flushing messages...", flush=True)
    running = False

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def delivery_report(err, msg):
    """Callback triggered on message delivery success or failure."""
    if err is not None:
        print(f"[Producer] Message delivery failed: {err}", file=sys.stderr, flush=True)

def clean_val(key, val):
    """Convert CSV string values to appropriate JSON data types."""
    if val == "NA" or val == "NaN" or val == "" or val is None:
        return None
    
    # Int columns
    int_cols = {"hour", "day", "month"}
    if key in int_cols:
        try:
            return int(val)
        except ValueError:
            return None
            
    # Float columns
    try:
        return float(val)
    except ValueError:
        # String
        return val.strip()

def get_event_stream(data_path):
    """Yield rows from CSV indefinitely, looping back to the beginning on EOF."""
    while running:
        if not os.path.exists(data_path):
            print(f"[Producer] Error: Dataset file not found at {data_path}", file=sys.stderr, flush=True)
            time.sleep(5)
            continue
            
        print(f"[Producer] Starting stream from dataset: {data_path}", flush=True)
        with open(data_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not running:
                    break
                yield row
        print("[Producer] Reached EOF. Looping back to the beginning of dataset...", flush=True)

def main():
    print("[Producer] Starting Boston Rideshare Kafka Producer...", flush=True)
    
    # Load environment variables
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "rideshare_events")
    dlq_topic = os.environ.get("KAFKA_DLQ_TOPIC", "rideshare_events_dlq")
    mode = os.environ.get("MODE", "demo").lower()
    
    # Determine event rate
    default_rates = {"demo": 50, "benchmark": 500, "stress": 1000}
    event_rate_str = os.environ.get("EVENT_RATE")
    if event_rate_str:
        try:
            event_rate = float(event_rate_str)
        except ValueError:
            event_rate = float(default_rates.get(mode, 50))
    else:
        event_rate = float(default_rates.get(mode, 50))
        
    data_path = os.environ.get("DATA_PATH", "/app/data/rideshare_kaggle.csv")
    
    print(f"[Producer] Configurations:")
    print(f"  - Bootstrap Servers: {bootstrap_servers}")
    print(f"  - Target Topic     : {topic}")
    print(f"  - DLQ Topic        : {dlq_topic}")
    print(f"  - Mode             : {mode}")
    print(f"  - Target Rate      : {event_rate} events/sec")
    print(f"  - Dataset Path     : {data_path}")
    
    # Ensure topics exist before starting
    create_topics_if_not_exists(bootstrap_servers, topic, dlq_topic)
    
    # Configure Kafka Producer
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'boston-rideshare-producer',
        'acks': '1',  # Standard ack level for throughput/durability balance
        'compression.type': 'lz4',
        'linger.ms': 10,  # Small delay to batch messages
        'batch.num.messages': 1000
    }
    
    try:
        producer = Producer(conf)
    except Exception as e:
        print(f"[Producer] Failed to create Kafka Producer: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
        
    start_time = time.time()
    sent_count = 0
    
    # Start streaming
    for raw_row in get_event_stream(data_path):
        try:
            # 1. Parse and clean raw data
            event = {k: clean_val(k, v) for k, v in raw_row.items()}
            
            # 2. Update timestamps and IDs for real-time simulation
            curr_time = time.time()
            dt = datetime.fromtimestamp(curr_time)
            
            event['id'] = str(uuid.uuid4())
            event['timestamp'] = curr_time
            event['datetime'] = dt.strftime('%Y-%m-%d %H:%M:%S')
            event['hour'] = dt.hour
            event['day'] = dt.day
            event['month'] = dt.month
            
            # 3. Extract partitioning key (keyed by source neighborhood)
            key = event.get("source", "unknown")
            if not key:
                key = "unknown"
                
            # 4. Serialize to JSON and publish
            val_bytes = json.dumps(event).encode('utf-8')
            key_bytes = key.encode('utf-8')
            
            producer.produce(
                topic=topic,
                value=val_bytes,
                key=key_bytes,
                callback=delivery_report
            )
            
            sent_count += 1
            
            # Periodically poll to trigger callbacks
            producer.poll(0)
            
            # Periodic logging
            log_interval = int(event_rate) * 10 or 100
            if sent_count % log_interval == 0:
                elapsed = time.time() - start_time
                actual_rate = sent_count / elapsed
                print(f"[Producer] Sent {sent_count} events. Actual rate: {actual_rate:.2f} events/sec", flush=True)
                
            # Precise rate limiting
            expected_time = start_time + (sent_count / event_rate)
            sleep_time = expected_time - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)
            elif sleep_time < -2.0:
                # Behind by more than 2 seconds, reset baseline to avoid huge burst
                print("[Producer] Warning: Behind schedule. Resetting rate-limiter baseline.", flush=True)
                start_time = time.time()
                sent_count = 0
                
        except Exception as e:
            print(f"[Producer] Error processing event: {e}", file=sys.stderr, flush=True)
            
    # Clean shutdown
    print("[Producer] Flushing remaining messages...", flush=True)
    producer.flush(timeout=5.0)
    print("[Producer] Shutdown complete.", flush=True)

if __name__ == "__main__":
    main()
