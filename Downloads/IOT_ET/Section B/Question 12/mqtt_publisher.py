"""
MQTT Publisher - Simulated Temperature Sensor
Publishes temperature readings every 2 seconds to sensors/temperature topic
"""

import json
import time
import random
from datetime import datetime

# pip install paho-mqtt
import paho.mqtt.client as mqtt

# Configuration
BROKER = "localhost"
PORT = 1883
TOPIC = "sensors/temperature"
CLIENT_ID = "sensor_publisher_01"


def on_connect(client, userdata, flags, rc, properties=None):
    """Called when connected to MQTT broker"""
    if rc == 0:
        print(f"[*] Connected to MQTT broker at {BROKER}:{PORT}")
    else:
        print(f"[!] Connection failed with code {rc}")


def on_disconnect(client, userdata, rc, properties=None):
    """Called when disconnected from MQTT broker"""
    print("[*] Disconnected from MQTT broker")


def generate_reading():
    """Generate a simulated temperature reading"""
    device_id = "sensor_01"
    # Simulate realistic temperature between 20.0 and 30.0 Celsius
    value = round(random.uniform(20.0, 30.0), 1)
    # Current Unix timestamp
    timestamp = int(datetime.now().timestamp())

    return {
        "device_id": device_id,
        "value": str(value),
        "ts": timestamp
    }


def main():
    # Create MQTT client
    client = mqtt.Client(client_id=CLIENT_ID)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # Connect to broker
    try:
        client.connect(BROKER, PORT, 60)
    except Exception as e:
        print(f"[!] Cannot connect to broker: {e}")
        return

    client.loop_start()  # Start network loop in background

    print("[*] Publishing temperature readings every 2 seconds...")
    print("[*] Press Ctrl+C to stop\n")

    try:
        while True:
            reading = generate_reading()
            payload = json.dumps(reading)

            # Publish to MQTT topic
            result = client.publish(TOPIC, payload)

            if result.is_published():
                print(f"[>] Published: {payload}")
            else:
                print(f"[!] Failed to publish: {payload}")

            # Wait 2 seconds before next reading
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n[*] Stopping publisher...")
    finally:
        client.loop_stop()
        client.disconnect()
        print("[*] Publisher stopped")


if __name__ == "__main__":
    main()