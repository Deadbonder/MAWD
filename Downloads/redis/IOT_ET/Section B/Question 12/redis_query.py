"""
Redis Query Script
Retrieves the last 10 readings for sensor_01 and computes average temperature
"""

import json
import redis

# Configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
DEVICE_ID = "sensor_01"
READINGS_TO_GET = 10

# Connect to Redis
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Key for this device's readings
redis_key = f"sensor:{DEVICE_ID}:readings"


def get_last_n_readings(n=10):
    """
    Get the last N readings from Redis
    Returns list of reading dictionaries
    """
    # LRANGE gets elements from index 0 to n-1 (newest readings)
    readings_raw = redis_client.lrange(redis_key, 0, n - 1)

    readings = []
    for raw in readings_raw:
        data = json.loads(raw)
        readings.append({
            "value": float(data["value"]),
            "ts": data["ts"]
        })

    return readings


def calculate_average(readings):
    """Calculate average temperature from readings"""
    if not readings:
        return 0.0

    total = sum(r["value"] for r in readings)
    return round(total / len(readings), 2)


def main():
    print(f"[*] Querying Redis for sensor: {DEVICE_ID}")
    print(f"[*] Getting last {READINGS_TO_GET} readings...\n")

    # Get readings
    readings = get_last_n_readings(READINGS_TO_GET)

    if not readings:
        print("[!] No readings found for this sensor")
        return

    # Display readings
    print("Last Readings:")
    print("-" * 40)
    for i, r in enumerate(readings, 1):
        print(f"  {i}. Value: {r['value']} | TS: {r['ts']}")

    # Calculate average
    avg_temp = calculate_average(readings)

    print("-" * 40)
    print(f"[*] Average Temperature: {avg_temp}")
    print(f"[*] Based on {len(readings)} readings")

    # Also show total readings stored
    total = redis_client.llen(redis_key)
    print(f"[*] Total readings stored for {DEVICE_ID}: {total}")


if __name__ == "__main__":
    main()