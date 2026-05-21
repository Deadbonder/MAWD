# Q13: Kafka-Based Log Processing Pipeline

## Overview
```
app.log → Kafka Producer → Kafka Topic (app-logs) → Kafka Consumer → errors_only.log
```

## Components

### A. Kafka Producer (kafka_producer.py)
- Reads lines from `app.log`
- Publishes each line as message to `app-logs` topic
- Creates sample `app.log` if file not found (graceful error handling)
- Continuously monitors for new lines

### B. Kafka Consumer (kafka_consumer.py)
- Consumes from `app-logs` topic
- Filters lines containing `ERROR`
- Appends filtered lines to `errors_only.log`
- Adds timestamp to each logged error

### C. Monitor Script (monitor.sh)
- Runs every 30 seconds
- Checks if consumer process is alive
- Auto-restarts if not running
- Logs all events to `monitor.log` with timestamps

### D. auto.offset.reset Explained

| Setting | Behavior |
|---------|----------|
| `earliest` | Start reading from the **beginning** of the topic (first message ever) |
| `latest` | Start reading from the **end** of the topic (only new messages) |

**When to use:**
- `earliest` = First time running, want to process all historical data
- `latest` = Already processed before, only care about new messages

**Example:**
```python
consumer = KafkaConsumer(
    TOPIC,
    auto_offset_reset="earliest"  # or "latest"
)
```

## Running the Pipeline

### Prerequisites
```bash
pip install kafka-python
```

### Steps
```bash
# 1. Start Kafka (Zookeeper + Kafka broker)
docker-compose up -d

# 2. Make monitor script executable
chmod +x monitor.sh

# 3. Create sample log file (or let producer create it)
echo -e "INFO: App started\nERROR: Failed to connect\nINFO: Retry" > app.log

# 4. Start Consumer (optionally via monitor.sh)
python3 kafka_consumer.py &

# 5. Start Producer
python3 kafka_producer.py

# 6. Check errors
cat errors_only.log
cat monitor.log
```