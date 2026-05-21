"""
Kafka Producer - Reads log file and publishes to Kafka topic
"""

import os
import time
from datetime import datetime

# pip install kafka-python
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "app-logs"
LOG_FILE = "app.log"


def on_send_success(record_metadata):
    """Called when message is sent successfully"""
    print(f"[*] Sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")


def on_send_error(exc):
    """Called when message send fails"""
    print(f"[!] Send failed: {exc}")


def read_and_publish():
    """Read log file and publish each line to Kafka"""
    try:
        # Check if file exists
        if not os.path.exists(LOG_FILE):
            print(f"[!] Error: Log file '{LOG_FILE}' not found!")
            print("[*] Creating sample app.log for testing...")
            with open(LOG_FILE, "w") as f:
                f.write("INFO: Application started\n")
                f.write("INFO: User logged in\n")
                f.write("ERROR: Database connection failed\n")
                f.write("INFO: Retry attempted\n")
                f.write("ERROR: Authentication failed for user admin\n")
                f.write("INFO: Connection restored\n")
            print(f"[*] Created sample {LOG_FILE}")

    except Exception as e:
        print(f"[!] Error checking log file: {e}")
        return False

    try:
        # Create Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
            acks="all"
        )

        print(f"[*] Producer connected to {KAFKA_BROKER}")
        print(f"[*] Reading from {LOG_FILE}...\n")

        # Track file position to only read new lines
        last_position = 0
        if os.path.exists(LOG_FILE):
            last_position = os.path.getsize(LOG_FILE)

        while True:
            try:
                # Check if file exists
                if not os.path.exists(LOG_FILE):
                    print(f"[!] Log file '{LOG_FILE}' not found, waiting...")
                    time.sleep(5)
                    continue

                with open(LOG_FILE, "r") as f:
                    # Seek to last position
                    f.seek(last_position)

                    # Read new lines
                    for line in f:
                        line = line.strip()
                        if line:
                            # Send to Kafka
                            producer.send(
                                TOPIC,
                                value=line.encode("utf-8")
                            ).add_callback(on_send_success).add_errback(on_send_error)
                            print(f"[>] {line}")

                    # Update position
                    last_position = f.tell()

                time.sleep(2)  # Check for new lines every 2 seconds

            except FileNotFoundError:
                print(f"[!] Log file '{LOG_FILE}' not found, waiting...")
                time.sleep(5)
            except Exception as e:
                print(f"[!] Error reading file: {e}")
                time.sleep(5)

    except Exception as e:
        print(f"[!] Kafka producer error: {e}")
        return False


if __name__ == "__main__":
    print("=" * 50)
    print("Kafka Log Producer")
    print("=" * 50)
    read_and_publish()