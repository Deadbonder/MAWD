"""
Kafka Consumer - Reads from app-logs topic, filters ERROR lines, writes to errors_only.log
"""

from datetime import datetime

# pip install kafka-python
from kafka import KafkaConsumer

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "app-logs"
OUTPUT_FILE = "errors_only.log"
FILTER_KEYWORD = "ERROR"


def main():
    try:
        # Create Kafka consumer
        # auto_offset_reset="earliest" means start from beginning if no offset saved
        # If you want to start from newest, use "latest"
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="earliest",
            group_id="error-filter-group",
            consumer_timeout_ms=10000  # Stop after 10 seconds of no messages
        )

        print(f"[*] Consumer connected to {KAFKA_BROKER}")
        print(f"[*] Listening on topic: {TOPIC}")
        print(f"[*] Filtering for keyword: '{FILTER_KEYWORD}'")
        print(f"[*] Writing errors to: {OUTPUT_FILE}\n")

        error_count = 0

        for message in consumer:
            # Handle both bytes and string values
            line = message.value
            if isinstance(line, bytes):
                line = line.decode("utf-8")
            line = line.strip()

            if FILTER_KEYWORD in line:
                # Append to output file
                with open(OUTPUT_FILE, "a") as f:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    f.write(f"[{timestamp}] {line}\n")

                error_count += 1
                print(f"[!] ERROR FOUND: {line}")

        if error_count == 0:
            print("[*] No ERROR lines found in messages")
        else:
            print(f"\n[*] Total errors logged: {error_count}")
            print(f"[*] Check {OUTPUT_FILE} for details")

    except KeyboardInterrupt:
        print("\n[*] Consumer stopped by user")
    except Exception as e:
        print(f"[!] Consumer error: {e}")
        print("[*] Make sure Kafka is running (docker-compose up -d)")


if __name__ == "__main__":
    print("=" * 50)
    print("Kafka Log Consumer (ERROR Filter)")
    print("=" * 50)
    main()