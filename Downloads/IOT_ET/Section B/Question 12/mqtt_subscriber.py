"""
MQTT Subscriber + Redis Storage
Receives sensor messages and stores them in Redis using LPUSH per device
Each device's list is trimmed to keep only the last 100 readings
"""

import json
from datetime import datetime

# pip install paho-mqtt redis
import paho.mqtt.client as mqtt
import redis

# Configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "sensors/temperature"

REDIS_HOST = "localhost"
REDIS_PORT = 6379
MAX_READINGS = 100  # Keep last 100 readings per device

redis_client = None


def on_connect(client, userdata, flags, rc, properties=None):
    """Called when connected to MQTT broker"""
    if rc == 0:
        print(f"[*] Connected to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
        # Subscribe to temperature topic
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"[*] Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"[!] Connection failed with code {rc}")


def on_message(client, userdata, msg, properties=None):
    """Called when a message is received on subscribed topic"""
    try:
        # Decode and parse JSON message
        payload = msg.payload.decode()
        data = json.loads(payload)

        device_id = data.get("device_id", "unknown")
        value = data.get("value", "0")
        timestamp = data.get("ts", 0)

        # Create Redis key for this device's readings
        redis_key = f"sensor:{device_id}:readings"

        # Create reading entry with timestamp for ordering
        reading_entry = json.dumps({
            "value": value,
            "ts": timestamp,
            "received_at": datetime.now().isoformat()
        })

        # Store in Redis list (LPUSH adds to beginning, newest first)
        redis_client.lpush(redis_key, reading_entry)

        # Trim list to keep only last MAX_READINGS entries
        redis_client.ltrim(redis_key, 0, MAX_READINGS - 1)

        print(f"[+] Stored reading for {device_id}: value={value}, ts={timestamp}")

    except json.JSONDecodeError:
        print(f"[!] Invalid JSON received: {msg.payload.decode()}")
    except Exception as e:
        print(f"[!] Error processing message: {e}")


def on_disconnect(client, userdata, rc, properties=None):
    """Called when disconnected from MQTT broker"""
    print("[*] Disconnected from MQTT broker")


def main():
    global redis_client

    # Connect to Redis
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        redis_client.ping()  # Test connection
        print(f"[*] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        print(f"[!] Cannot connect to Redis: {e}")
        return

    # Create MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    # Connect to MQTT broker
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        print(f"[!] Cannot connect to MQTT broker: {e}")
        return

    print("[*] Subscriber running... Press Ctrl+C to stop\n")

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[*] Stopping subscriber...")
    finally:
        client.disconnect()
        redis_client.close()
        print("[*] Subscriber stopped")


if __name__ == "__main__":
    main()