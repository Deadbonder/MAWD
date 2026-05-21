# Q2: Real-Time IoT Sensor Data Pipeline

## Overview
MQTT (transport) → Redis (storage) pipeline for temperature sensor data

## Components

### A. MQTT Publisher (mqtt_publisher.py)
- Simulates temperature sensor
- Publishes JSON to `sensors/temperature` every 2 seconds
- Format: `{"device_id":"sensor_01","value":"27.5","ts":1700000000}`

### B. MQTT Subscriber (mqtt_subscriber.py)
- Subscribes to `sensors/temperature` topic
- Stores readings in Redis Lists per device
- Uses LPUSH + LTRIM to keep last 100 readings
- Key pattern: `sensor:{device_id}:readings`

### C. Redis Query (redis_query.py)
- LRANGE to get last 10 readings
- Calculates average temperature
- Shows all readings with timestamps

### D. Unique Device IDs - Answer
**Use: Redis SET**

```python
redis_client.sadd("devices:today", device_id)
```

**Justification:**
- SET automatically ensures uniqueness (SADD ignores duplicates)
- O(1) add and lookup
- Perfect for tracking which devices reported today without duplicates
- Alternative: could use bitmap if device IDs are numeric IDs, but SET is simpler for string IDs

## Running the Pipeline

### Prerequisites
Install the required Python dependencies:
```bash
pip install -r requirements.txt
```

### 1. Start Infrastructure (Redis & MQTT Broker) with Docker
Navigate to the directory containing `docker-compose.yml` and run:
```bash
docker-compose up -d
```
This will pull and run both Redis (on port 6379) and the Mosquitto MQTT broker (on port 1883) in the background.

### 2. Start Subscriber (receives and stores in Redis)
```bash
python3 mqtt_subscriber.py
```

### 3. Start Publisher (generates simulated data)
```bash
python3 mqtt_publisher.py
```

### 4. Query Results
```bash
python3 redis_query.py
```