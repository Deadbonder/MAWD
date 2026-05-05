# IoT Telemetry Pipeline & Analytical Dashboard — Low-Level Design

**Version:** 1.0
**Date:** 2026-05-04
**Status:** Draft

---

## 1. MQTT Topic Naming Conventions & JSON Payload Schema

### 1.1 Topic Structure

```
aeXXXXXX/<sensor_id>/<reading_type>
```

| Segment | Description | Example |
|---------|-------------|---------|
| `aeXXXXXX` | Organization/project prefix (fixed) | `aeXXXXXX` |
| `<sensor_id>` | Unique sensor identifier | `sensor_01`, `temp_02` |
| `<reading_type>` | Type of measurement | `temperature`, `humidity`, `pressure`, `battery` |

**Examples:**
- `aeXXXXXX/sensor_01/temperature`
- `aeXXXXXX/sensor_02/humidity`
- `aeXXXXXX/sensor_03/battery`

### 1.2 Expected JSON Payload Schema

```json
{
  "sensor_id": "sensor_01",
  "timestamp": "2026-05-04T14:30:00.000Z",
  "reading_type": "temperature",
  "value": 23.7,
  "unit": "°C",
  "metadata": {
    "firmware_version": "1.2.3",
    "location": "zone-a"
  }
}
```

#### Field Definitions

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sensor_id` | string | Yes | Unique sensor identifier; must match topic segment |
| `timestamp` | ISO 8601 string | Yes | UTC timestamp of when reading was taken at the sensor |
| `reading_type` | string | Yes | Measurement category (temperature, humidity, pressure, etc.) |
| `value` | float | Yes | The measured value |
| `unit` | string | Yes | Unit of measurement (°C, %, Pa, V, etc.) |
| `metadata` | object | No | Optional sensor metadata (firmware version, location, etc.) |

### 1.3 Payload Validation Rules

- `sensor_id`: 1–64 characters, alphanumeric + underscore
- `timestamp`: Must be a valid ISO 8601 UTC datetime; past timestamps up to 24 hours old are accepted; future timestamps are rejected (with a 30-second clock skew tolerance to account for minor sensor clock drift)
- `value`: Must be a finite float; `NaN` and `Infinity` are rejected
- `unit`: Non-empty string, max 16 characters

### 1.4 Enriched Payload Schema (Post Anomaly Detection)

After the anomaly engine processes a reading, the payload is enriched with:

```json
{
  "sensor_id": "sensor_01",
  "timestamp": "2026-05-04T14:30:00.000Z",
  "reading_type": "temperature",
  "value": 23.7,
  "unit": "°C",
  "metadata": {
    "firmware_version": "1.2.3",
    "location": "zone-a"
  },
  "anomaly": {
    "score": -0.82,
    "is_anomaly": true,
    "confidence": 0.91,
    "contributing_features": ["value_spike", "rate_of_change"],
    "model_version": "iforest_v1_2026-05-04"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `anomaly.score` | float | Raw Isolation Forest score. Range: -1 (anomaly) to +1 (normal) |
| `anomaly.is_anomaly` | bool | `true` if score < threshold (-0.5 default) |
| `anomaly.confidence` | float | Normalized confidence: `abs(score)` mapped to 0–1 |
| `anomaly.contributing_features` | string[] | Which extracted features drove the anomaly |
| `anomaly.model_version` | string | Model identifier for audit trail |

---

## 2. Time-Series Database Schema Design

### 2.1 Database: InfluxDB 2.7

**Organization:** `iot-project`
**Bucket:** `sensor-data`
**Retention Policy:** 30 days (configurable)

### 2.2 Measurement: `sensor_readings`

| Column | Type | Tag/Field | Description |
|--------|------|-----------|-------------|
| `sensor_id` | string | **Tag** | Sensor identifier (indexed for queries) |
| `reading_type` | string | **Tag** | Measurement type (indexed) |
| `timestamp` | datetime | **Timestamp** | When reading was taken (from sensor payload) |
| `processed_at` | datetime | **Field** | When reading was flushed to DB (server-side) |
| `value` | float | **Field** | Numeric measurement value |
| `unit` | string | **Field** | Unit of measurement |
| `location` | string | **Tag** (from metadata) | Sensor location |
| `firmware_version` | string | **Tag** (from metadata) | Sensor firmware version |
| `anomaly_score` | float | **Field** | Raw isolation forest score (-1 anomaly to +1 normal) |
| `is_anomaly` | bool | **Field** | Whether reading was flagged as anomalous |
| `anomaly_confidence` | float | **Field** | Normalized confidence 0–1 |

### 2.3 InfluxDB Line Protocol (for batch write)

```
sensor_readings,sensor_id=sensor_01,reading_type=temperature,location=zone-a,firmware_version=1.2.3 timestamp=1743778800000000000,processed_at=1743779100000000000,value=23.7,unit="°C"
```

**Timestamp precision:** Nanoseconds (InfluxDB default)
**Processed_at:** Unix nanoseconds at the moment of flush

### 2.4 Example Flux Queries

**Latest reading per sensor:**
```flux
from(bucket: "sensor-data")
  |> range(start: -1h)
  |> filter(fn: (r) => r["_measurement"] == "sensor_readings")
  |> last()
```

**Aggregate KPIs (min, max, avg) over last 24 hours per sensor:**
```flux
from(bucket: "sensor-data")
  |> range(start: -24h)
  |> filter(fn: (r) => r["_measurement"] == "sensor_readings")
  |> group(columns: ["sensor_id", "reading_type"])
  |> reduce(identity: {count: 0, sum: 0.0, min: 0.0, max: 0.0},
    fn: (r, accumulator) => ({
      count: accumulator.count + 1.0,
      sum: accumulator.sum + r.value,
      min: if accumulator.count == 0.0 then r.value else
           if r.value < accumulator.min then r.value else accumulator.min,
      max: if accumulator.count == 0.0 then r.value else
           if r.value > accumulator.max then r.value else accumulator.max
    })
  )
```

---

## 3. FastAPI REST & WebSocket Endpoint Contracts

### 3.1 Base URL

```
http://localhost:3000/api/v1
```

### 3.2 REST Endpoints

#### `GET /api/v1/sensors`
Returns list of all known sensor IDs.

**Response `200 OK`:**
```json
{
  "sensors": [
    {"sensor_id": "sensor_01", "reading_types": ["temperature", "humidity"]},
    {"sensor_id": "sensor_02", "reading_types": ["temperature", "pressure"]}
  ],
  "count": 2
}
```

---

#### `GET /api/v1/sensors/{sensor_id}/data`
Returns historical readings for a sensor within a time range.

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `sensor_id` | string | The sensor identifier |

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start` | ISO 8601 datetime | No | `-24h` | Start of time range (inclusive) |
| `end` | ISO 8601 datetime | No | `now` | End of time range (inclusive) |
| `reading_type` | string | No | all | Filter by reading type |
| `interval` | string | No | `5m` | Downsample interval (e.g., `5m`, `1h`, `1d`) |
| `limit` | integer | No | `1000` | Max records returned (max 10000) |
| `offset` | integer | No | `0` | Pagination offset |

**Response `200 OK`:**
```json
{
  "sensor_id": "sensor_01",
  "reading_type": "temperature",
  "data": [
    {
      "timestamp": "2026-05-04T12:00:00.000Z",
      "processed_at": "2026-05-04T12:05:00.000Z",
      "value": 22.3,
      "unit": "°C"
    },
    {
      "timestamp": "2026-05-04T12:05:00.000Z",
      "processed_at": "2026-05-04T12:10:00.000Z",
      "value": 22.7,
      "unit": "°C"
    }
  ],
  "total": 2,
  "limit": 1000,
  "offset": 0
}
```

**Response `404 Not Found`:**
```json
{
  "detail": "Sensor sensor_99 not found"
}
```

---

#### `GET /api/v1/sensors/{sensor_id}/latest`
Returns the most recent reading for a sensor. Fetched directly from Redis for sub-second response times.

**Logic:** Queries `redis_client.get(f"latest:{sensor_id}:{reading_type}")`. Falls back to InfluxDB if the key is not found in Redis.

**Response `200 OK`:**
```json
{
  "sensor_id": "sensor_01",
  "reading_type": "temperature",
  "timestamp": "2026-05-04T14:30:00.000Z",
  "processed_at": "2026-05-04T14:35:00.000Z",
  "value": 23.7,
  "unit": "°C",
  "data_age_seconds": 3
}
```

> **`data_age_seconds`**: Integer. Seconds between the sensor's `timestamp` and the current server time. Since this value is read directly from the Redis speed layer (written on every MQTT message), this reflects the true age of the data — typically just a few seconds, not the full 5-minute batch interval.

---

#### `GET /api/v1/kpis`
Returns aggregate KPIs (min, max, avg, count) per sensor over a configurable window.

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `window` | string | No | `1h` | Time window (e.g., `1h`, `6h`, `24h`, `7d`) |
| `reading_type` | string | No | all | Filter by reading type |

**Response `200 OK`:**
```json
{
  "window": "24h",
  "kpis": [
    {
      "sensor_id": "sensor_01",
      "reading_type": "temperature",
      "min": 18.2,
      "max": 27.3,
      "avg": 22.6,
      "count": 288,
      "unit": "°C"
    }
  ]
}
```

---

#### `GET /health`
Health check endpoint for the ingestion and API services.

**Ingestion service response (`GET http://ingestion:8001/health`):**
```json
{
  "status": "ok",
  "buffer_size": 10,
  "last_flush_at": "2026-05-04T14:35:00.000Z",
  "next_flush_at": "2026-05-04T14:40:00.000Z",
  "mqtt_connected": true,
  "subscribed_topic": "aeXXXXXX/#",
  "circuit_breaker": {
    "circuit_breaker": "influxdb",
    "state": "closed",
    "failure_count": 0,
    "success_count": 1440,
    "total_rejected": 0
  },
  "disk_buffer_files": 0
}
```

**API service response (`GET http://api:8000/health`):**
```json
{
  "status": "ok",
  "influxdb_connected": true,
  "timestamp": "2026-05-04T14:36:00.000Z"
}
```

---

### 3.3 WebSocket Endpoint

#### `WS /ws/live`

Pushes the latest sensor readings to connected clients after each database flush (every 5 minutes).

**Connection:** `ws://localhost:3000/ws/live`

**Server -> Client Message (JSON):**
```json
{
  "event": "flush_complete",
  "timestamp": "2026-05-04T14:40:00.000Z",
  "readings": [
    {
      "sensor_id": "sensor_01",
      "reading_type": "temperature",
      "timestamp": "2026-05-04T14:35:00.000Z",
      "processed_at": "2026-05-04T14:40:00.000Z",
      "value": 23.7,
      "unit": "°C"
    },
    {
      "sensor_id": "sensor_02",
      "reading_type": "humidity",
      "timestamp": "2026-05-04T14:35:00.000Z",
      "processed_at": "2026-05-04T14:40:00.000Z",
      "value": 58.1,
      "unit": "%"
    }
  ]
}
```

**Client -> Server (Ping/Pong for keepalive):**
```json
{ "type": "ping" }
```

**Server -> Client (Pong):**
```json
{ "type": "pong" }
```

**Connection lifecycle:**
- On successful WebSocket handshake, server sends `{ "event": "connected", "timestamp": "..." }`
- Server sends flush messages every 5 minutes (or on error condition)
- Client should handle reconnection on disconnect
- Ping/pong keepalive every 30 seconds

---

#### `GET /api/v1/anomalies`
Returns recent anomaly events detected by the inline ML engine.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `since` | ISO 8601 | `-1h` | Time window |
| `sensor_id` | string | all | Filter by sensor |
| `min_confidence` | float | `0.5` | Minimum confidence threshold |

**Response `200 OK`:**
```json
{
  "anomalies": [
    {
      "sensor_id": "sensor_01",
      "reading_type": "temperature",
      "value": 47.2,
      "timestamp": "2026-05-04T14:30:00.000Z",
      "anomaly": {
        "score": -0.82,
        "is_anomaly": true,
        "confidence": 0.91,
        "contributing_features": ["value_spike"]
      }
    }
  ],
  "count": 1
}
```

---

## 4. Python Processor — Buffer and Flush Sequence Diagram

```mermaid
sequenceDiagram
    participant Broker as External MQTT Broker
    participant Ingest as Python Ingestion Service
    participant AI as Anomaly Engine<br/>(Isolation Forest)
    participant Buffer as In-Memory Buffer<br/>(Dict[sensor_id → Dict[reading_type]])
    participant CB as Circuit Breaker
    participant Disk as Disk Spillover
    participant Redis as Redis Speed Layer
    participant DB as InfluxDB
    participant WS as WebSocket Clients

    Note over Broker,Ingest: Continuous MQTT subscription (no timer)
    Broker->>Ingest: MQTT Message on aeXXXXXX/sensor_01/temperature
    Ingest->>Ingest: Parse JSON payload
    Ingest->>AI: Extract features + score anomaly (<2ms)
    AI-->>Ingest: {score: -0.82, is_anomaly: true}

    par Write to Redis + Buffer simultaneously
        Ingest->>Redis: SET latest:sensor_01:temperature {enriched payload}
        Ingest->>Buffer: buffer[sensor_01][temperature] = {enriched payload, last_seen}
    end

    alt is_anomaly == true
        Ingest->>Redis: PUBLISH alerts:anomaly {alert}
        Redis->>WS: Forward anomaly alert immediately
    end

    Note over Scheduler,CB: APScheduler fires every 300s
    Scheduler->>CB: Check circuit state
    alt Circuit CLOSED or HALF_OPEN
        CB->>DB: Batch write all buffered readings
        DB-->>CB: Write confirmed
        CB->>CB: record_success()
        CB->>Buffer: buffer.clear()
        CB->>WS: emit flush_complete event
        CB->>Disk: drain any disk-buffered batches (FIFO)
    else Circuit OPEN
        CB->>Disk: Spill readings to JSON file
        Note over Disk: Durable overflow — no DB contact
    end

    Note over Broker,WS: Next MQTT message arrives at any time,<br/>updating buffer independently of flush cycle
```

### 4.1 Key Implementation Details

**Ingestion Service Main Loop (pseudocode):**
```python
import paho.mqtt.client as mqtt
from apscheduler.schedulers.background import BackgroundScheduler
import redis
import json
import time
import os
from pathlib import Path
from enum import Enum
from threading import Lock
from dataclasses import dataclass, field

# ─────────────────────────────────────────────
# OpenTelemetry Tracing Setup
# ─────────────────────────────────────────────
from opentelemetry import trace, context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.trace import SpanKind, StatusCode

_tracer = None

def init_telemetry(service_name: str):
    global _tracer
    resource = Resource(attributes={SERVICE_NAME: service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(
        endpoint=os.getenv("OTEL_EXPORTER_ENDPOINT", "http://otel-collector:4317"),
        insecure=True
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer(service_name)

init_telemetry("iot-ingestion-service")

# ─────────────────────────────────────────────
# Circuit Breaker (resilience/circuit_breaker.py)
# ─────────────────────────────────────────────
class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class CircuitBreaker:
    name: str
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)
    _success_count: int = field(default=0, init=False)
    _total_rejected: int = field(default=0, init=False)
    _lock: Lock = field(default_factory=Lock, init=False)

    @property
    def state(self) -> CircuitState:
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
            return self._state

    def can_execute(self) -> bool:
        current = self.state
        if current in (CircuitState.CLOSED, CircuitState.HALF_OPEN):
            return True
        self._total_rejected += 1
        return False

    def record_success(self):
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count += 1

    def record_failure(self):
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN

    def get_metrics(self) -> dict:
        return {
            "circuit_breaker": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "total_rejected": self._total_rejected,
        }

influx_breaker = CircuitBreaker(
    name="influxdb",
    failure_threshold=int(os.getenv("CB_FAILURE_THRESHOLD", "5")),
    recovery_timeout=float(os.getenv("CB_RECOVERY_TIMEOUT", "60")),
)

# ─────────────────────────────────────────────
# Anomaly Detection (anomaly/scorer.py)
# ─────────────────────────────────────────────
class AnomalyScorer:
    def __init__(self, model_path: str, threshold: float = -0.5):
        self.threshold = threshold
        self.model = None
        self.model_version = "none"
        self._load_model(model_path)

    def _load_model(self, path: str):
        import joblib
        p = Path(path)
        if p.exists():
            self.model = joblib.load(p)
            self.model_version = p.stem
            log_info(f"Loaded anomaly model: {self.model_version}")
        else:
            log_warning(f"No model at {path} — anomaly detection disabled")

    def score(self, features) -> dict:
        import numpy as np
        if self.model is None:
            return {"score": 0.0, "is_anomaly": False, "confidence": 0.0,
                    "contributing_features": [], "model_version": "disabled"}
        try:
            raw_score = float(self.model.decision_function(features.reshape(1, -1))[0])
            is_anomaly = raw_score < self.threshold
            confidence = min(abs(raw_score) / abs(self.threshold), 1.0)
            contributing = []
            if len(features) >= 4 and abs(features[3]) > 0.5:
                contributing.append("rate_of_change")
            return {
                "score": round(raw_score, 4),
                "is_anomaly": is_anomaly,
                "confidence": round(confidence, 4),
                "contributing_features": contributing,
                "model_version": self.model_version,
            }
        except Exception as e:
            log_error(f"Anomaly scoring failed: {e}")
            return {"score": 0.0, "is_anomaly": False, "confidence": 0.0,
                    "contributing_features": [], "model_version": "error"}

anomaly_scorer = AnomalyScorer(
    model_path=os.getenv("ANOMALY_MODEL_PATH", "/models/iforest.joblib"),
    threshold=float(os.getenv("ANOMALY_THRESHOLD", "-0.5"))
)

# Feature extractor: sliding window per (sensor_id, reading_type)
from collections import deque
import numpy as np

class SensorFeatureExtractor:
    def __init__(self, window_size: int = 20):
        self.windows: dict[tuple, deque] = {}
        self.window_size = window_size

    def extract(self, sensor_id: str, reading_type: str, value: float) -> np.ndarray:
        key = (sensor_id, reading_type)
        if key not in self.windows:
            self.windows[key] = deque(maxlen=self.window_size)
        w = self.windows[key]
        w.append(value)
        vals = np.array(w)
        if len(vals) < 3:
            return np.zeros(6)
        return np.array([
            value,
            np.mean(vals),
            np.std(vals),
            value - vals[-2] if len(vals) > 1 else 0,
            value - np.min(vals),
            np.max(vals) - value,
        ])

feature_extractor = SensorFeatureExtractor(window_size=20)

# ─────────────────────────────────────────────
# Buffer + State
# ─────────────────────────────────────────────
buffer: dict[str, dict[str, dict]] = {}
failed_batches: list[dict] = []

MAX_BUFFER_SIZE = 10000
STALE_THRESHOLD_SECONDS = 600
DISK_BUFFER_DIR = Path(os.getenv("DISK_BUFFER_DIR", "/data/buffer"))
DISK_BUFFER_DIR.mkdir(parents=True, exist_ok=True)

redis_client = redis.Redis.from_url("redis://redis:6379", decode_responses=True)
scheduler = BackgroundScheduler()

# ─────────────────────────────────────────────
# MQTT Message Handler (with OTel + ML + Redis)
# ─────────────────────────────────────────────
def on_mqtt_message(client, userdata, msg):
    with _tracer.start_as_current_span(
        "mqtt_message_received",
        kind=SpanKind.CONSUMER,
        attributes={"mqtt.topic": msg.topic}
    ) as span:
        payload = json.loads(msg.payload)
        sensor_id = payload["sensor_id"]
        reading_type = payload.get("reading_type", "default")
        value = payload["value"]

        span.set_attribute("sensor.id", sensor_id)
        span.set_attribute("sensor.reading_type", reading_type)
        span.set_attribute("sensor.value", value)

        # — Anomaly scoring (traced) —
        with _tracer.start_as_current_span("anomaly_scoring") as ai_span:
            features = feature_extractor.extract(sensor_id, reading_type, value)
            anomaly_result = anomaly_scorer.score(features)
            payload["anomaly"] = anomaly_result
            ai_span.set_attribute("anomaly.score", anomaly_result["score"])
            ai_span.set_attribute("anomaly.is_anomaly", anomaly_result["is_anomaly"])

        # — Buffer insert —
        with _tracer.start_as_current_span("buffer_insert") as buf_span:
            if sensor_id not in buffer:
                buffer[sensor_id] = {}
            if len(buffer[sensor_id]) >= MAX_BUFFER_SIZE:
                log_warning(f"Buffer full for sensor {sensor_id}")
            else:
                buffer[sensor_id][reading_type] = {
                    "payload": payload,
                    "last_seen": time.time()
                }
            buf_span.set_attribute("buffer.total_sensors", len(buffer))

        # — Redis speed layer write (auto-instrumented) —
        with _tracer.start_as_current_span("redis_speed_layer_write"):
            redis_key = f"latest:{sensor_id}:{reading_type}"
            redis_client.set(redis_key, json.dumps(payload))

        # — Anomaly alert via Redis pub/sub —
        if anomaly_result["is_anomaly"]:
            alert = {
                "event": "anomaly_detected",
                "sensor_id": sensor_id,
                "reading_type": reading_type,
                "value": value,
                "anomaly": anomaly_result,
                "timestamp": payload["timestamp"]
            }
            redis_client.publish("alerts:anomaly", json.dumps(alert))

# ─────────────────────────────────────────────
# Flush Handler (with Circuit Breaker + Disk Spillover)
# ─────────────────────────────────────────────
def _spill_to_disk(readings: list):
    filename = DISK_BUFFER_DIR / f"batch_{int(time.time())}.json"
    with open(filename, "w") as f:
        json.dump(readings, f)
    log_info(f"Spilled {len(readings)} readings to {filename}")

def _drain_disk_buffer():
    files = sorted(DISK_BUFFER_DIR.glob("batch_*.json"))
    for f in files:
        try:
            with open(f) as fh:
                readings = json.load(fh)
            write_to_influxdb(readings)
            f.unlink()
            log_info(f"Drained disk batch {f.name}")
        except Exception as e:
            log_error(f"Disk drain failed on {f.name}: {e}")
            break

def flush_buffer():
    with _tracer.start_as_current_span(
        "influx_batch_flush",
        kind=SpanKind.CLIENT,
        attributes={"circuit_breaker.state": influx_breaker.state.value}
    ) as span:
        now = time.time()

        # 1. Evict stale entries
        stale_sensors = [
            sid for sid, entries in buffer.items()
            if all(now - e["last_seen"] > STALE_THRESHOLD_SECONDS for e in entries.values())
        ]
        for sid in stale_sensors:
            buffer.pop(sid, None)

        if not buffer:
            return

        flat_readings = [
            entry["payload"]
            for entries in buffer.values()
            for entry in entries.values()
        ]

        # 2. Circuit Breaker gate
        if influx_breaker.can_execute():
            try:
                with _tracer.start_as_current_span("influx_batch_write") as db_span:
                    write_to_influxdb(flat_readings)
                    db_span.set_attribute("batch.size", len(flat_readings))

                influx_breaker.record_success()
                buffer.clear()

                with _tracer.start_as_current_span("ws_broadcast") as ws_span:
                    broadcast_via_websocket(flat_readings)
                    ws_span.set_attribute("ws.connected_clients", len(ws_connections))

                # 3. Drain disk buffer on success
                _drain_disk_buffer()

            except Exception as e:
                span.set_status(StatusCode.ERROR, str(e))
                span.record_exception(e)
                influx_breaker.record_failure()
                _spill_to_disk(flat_readings)
                ws_emit("flush_error", {
                    "error": str(e),
                    "circuit_state": influx_breaker.state.value
                })
        else:
            # Circuit OPEN — skip DB, spill to disk
            log_warning(f"Circuit OPEN — buffering {len(flat_readings)} to disk")
            _spill_to_disk(flat_readings)

scheduler.add_job(flush_buffer, "interval", seconds=300)
client.on_message = on_mqtt_message
client.subscribe("aeXXXXXX/#")
client.loop_start()
scheduler.start()
```

---

## 5. Frontend Component Structure — Analytical Dashboard

### 5.1 Component Hierarchy

```
<App>
├── <DashboardLayout>
│   ├── <Header />
│   ├── <ViewToggle />            # Switch: "Charts" | "3D Twin"
│   ├── <KPIRibbon />             # (existing, always visible)
│   │
│   ├── <ChartView />             # Shown when mode === "charts"
│   │
│   └── <DigitalTwinView />        # Shown when mode === "twin"
│       ├── <R3F Canvas>
│       │   ├── <ambientLight />
│       │   ├── <directionalLight />
│       │   ├── <OrbitControls />
│       │   ├── <FloorPlan />         # Static floor/room mesh
│       │   ├── <SensorNode3D />      # One per active sensor
│       │   │   ├── <AnimatedMesh />   # Geometry reacts to value
│       │   │   ├── <StatusRing />     # Glow ring: green/yellow/red
│       │   │   └── <Html>             # drei overlay for label
│       │   └── <EffectComposer>      # Post-processing bloom
│       └── <SensorDetailPanel />     # Slide-out panel on click
```

### 5.2 Component Details

#### `<KPIRibbon />`
Horizontal scrolling strip at the top of the dashboard.

**Props:** `kpis: KPIData[]` (from `/api/v1/kpis`)

**Layout:** Flexbox row with horizontal scroll, gap-4 spacing.

**Per-Card Design:**
```
┌─────────────────────────────────┐
│  sensor_01 · temperature       │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │
│  23.7 °C           ▲ +0.3      │
│  min: 18.2  max: 27.3          │
│  Last updated: 14:40            │
└─────────────────────────────────┘
```

- **Value:** Large font, primary color
- **Trend arrow:** Green (▲ up), Red (▼ down), Gray (— flat) based on delta from previous reading
- **Min/Max:** Small secondary text
- **Timestamp:** Relative time ("2 min ago") or absolute ("14:40")

**Color coding:**
- Normal: Blue/gray border
- Warning (value > 90th percentile): Yellow border
- Critical (value > 98th percentile): Red border

#### `<ChartView />`
Time-series line chart below the KPI ribbon.

**Props:** `sensorId: string, readingType: string, timeRange: TimeRange`

**Features:**
- X-axis: Time (auto-scaled to time range)
- Y-axis: Value with unit label
- Multi-sensor overlay mode: toggle up to 3 sensors on same chart
- Hover tooltip: shows exact value and timestamp
- Chart library: **Recharts** (React-native, lightweight)

**States:**
- Loading: Skeleton placeholder
- Error: Error message with retry button
- Empty: "No data for selected range" message

#### `<TimeRangeSelector />`
Button group for selecting the time window.

**Options:** `1h`, `6h`, `24h`, `7d`

**Behavior:** Selecting a range triggers a re-fetch of `/api/v1/sensors/{id}/data` and re-renders the chart.

#### `<ConnectionStatus />`
Small indicator in the header showing WebSocket connection state.

**States:**
- Connected (green dot): WebSocket active, receiving live data
- Reconnecting (yellow dot): Connection lost, auto-retry in progress
- Disconnected (red dot): Manual reconnect button shown

### 5.3 API Integration

**Custom Hooks (React):**

```typescript
// useSensorData.ts
function useSensorData(sensorId: string, timeRange: TimeRange) {
  // fetches GET /api/v1/sensors/{sensorId}/data?start=&end=
  // returns { data, isLoading, error }
}

// useKPIs.ts
function useKPIs(window: TimeWindow) {
  // fetches GET /api/v1/kpis?window=
  // returns { kpis, isLoading, error }
}

// useWebSocket.ts
function useWebSocket(url: string) {
  // manages WebSocket connection lifecycle
  // returns { lastMessage, connectionStatus, reconnect }
}
```

### 5.4 State Management

- **No external state library** (Redux/Zustand) needed for this scope
- React Context for global state: `SensorContext` (list of sensors, selected sensor)
- Local component state for UI concerns (chart zoom, modal open/close)
- `useReducer` for complex `ChartView` state if needed

### 5.5 Key Files Structure

```
frontend/
├── src/
│   ├── components/
│   │   ├── DashboardLayout.tsx
│   │   ├── Header.tsx
│   │   ├── ViewToggle.tsx              # NEW — switches Charts ↔ 3D Twin
│   │   ├── KPIRibbon.tsx
│   │   ├── KPICard.tsx
│   │   ├── ChartView.tsx
│   │   ├── TimeRangeSelector.tsx
│   │   ├── ConnectionStatus.tsx
│   │   └── twin/                      # NEW — 3D Digital Twin
│   │       ├── DigitalTwinView.tsx     # Canvas + scene container
│   │       ├── SensorNode3D.tsx        # Generic sensor node (R3F)
│   │       ├── FanNode3D.tsx           # Fan-specific animated node
│   │       ├── GaugeNode3D.tsx         # Pressure gauge node
│   │       ├── FloorPlan.tsx           # Static floor/room geometry
│   │       ├── SensorDetailPanel.tsx   # Slide-out info panel
│   │       └── effects/
│   │           └── GlowEffect.tsx     # Reusable bloom/glow wrapper
│   ├── hooks/
│   │   ├── useSensorData.ts
│   │   ├── useKPIs.ts
│   │   └── useWebSocket.ts
│   ├── context/
│   │   └── SensorContext.tsx           # MODIFIED — enhanced reducer with anomaly status
│   ├── api/
│   │   └── client.ts
│   ├── types/
│   │   └── sensor.ts
│   ├── App.tsx
│   └── main.tsx
├── tailwind.config.js
├── vite.config.ts
└── package.json
```

---

## 5.6 3D Digital Twin — Implementation Details

### 5.6.1 Package Dependencies

```json
{
  "@react-three/fiber": "^8.15.0",
  "@react-three/drei": "^9.90.0",
  "@react-three/postprocessing": "^2.16.0",
  "three": "^0.160.0"
}
```

### 5.6.2 Core Component: SensorNode3D

```tsx
// components/twin/SensorNode3D.tsx
import { useRef, useMemo } from 'react'
import { useFrame } from '@react-three/fiber'
import { Html } from '@react-three/drei'
import * as THREE from 'three'
import { useSensorContext } from '../../context/SensorContext'

interface SensorNode3DProps {
  sensorId: string
  readingType: string
  position: [number, number, number]  // x, y, z in scene
  minValue: number   // expected min (e.g., 15°C)
  maxValue: number   // expected max (e.g., 45°C)
  onClick: (sensorId: string) => void
}

function valueToColor(value: number, min: number, max: number): THREE.Color {
  const t = THREE.MathUtils.clamp((value - min) / (max - min), 0, 1)
  const color = new THREE.Color()
  // 0.0 → hue 0.6 (blue/cold), 0.5 → hue 0.3 (green), 1.0 → hue 0.0 (red/hot)
  color.setHSL(0.6 - t * 0.6, 0.9, 0.5)
  return color
}

export function SensorNode3D({
  sensorId, readingType, position, minValue, maxValue, onClick
}: SensorNode3DProps) {
  const meshRef = useRef<THREE.Mesh>(null!)
  const glowRef = useRef<THREE.PointLight>(null!)
  const { sensorData } = useSensorContext()

  const reading = sensorData[sensorId]?.[readingType]
  const value = reading?.value ?? (minValue + maxValue) / 2

  useFrame((_, delta) => {
    if (!meshRef.current) return
    const targetColor = valueToColor(value, minValue, maxValue)
    const mat = meshRef.current.material as THREE.MeshStandardMaterial

    // Lerp color for smooth transitions (buttery-smooth, no snapping)
    mat.color.lerp(targetColor, delta * 3)
    mat.emissive.lerp(targetColor, delta * 3)
    mat.emissiveIntensity = THREE.MathUtils.lerp(
      mat.emissiveIntensity, 0.3 + ((value - minValue) / (maxValue - minValue)) * 0.7, delta * 3
    )

    // Pulse glow light intensity
    if (glowRef.current) {
      glowRef.current.intensity = 0.5 + Math.sin(Date.now() * 0.003) * 0.2
      glowRef.current.color.copy(targetColor)
    }
  })

  return (
    <group position={position}>
      <mesh ref={meshRef} onClick={() => onClick(sensorId)} castShadow>
        <sphereGeometry args={[0.3, 32, 32]} />
        <meshStandardMaterial
          color="#4488ff"
          emissive="#4488ff"
          emissiveIntensity={0.3}
          roughness={0.2}
          metalness={0.8}
        />
      </mesh>
      <pointLight ref={glowRef} intensity={0.5} distance={3} />
      <Html distanceFactor={10} position={[0, 0.5, 0]} center>
        <div className="sensor-label">
          <span className="sensor-id">{sensorId}</span>
          <span className="sensor-value">
            {value.toFixed(1)} {reading?.unit ?? ''}
          </span>
        </div>
      </Html>
    </group>
  )
}
```

### 5.6.3 Scene Container: DigitalTwinView

```tsx
// components/twin/DigitalTwinView.tsx
import { Canvas } from '@react-three/fiber'
import { OrbitControls, Environment, Grid } from '@react-three/drei'
import { EffectComposer, Bloom } from '@react-three/postprocessing'
import { SensorNode3D } from './SensorNode3D'
import { SensorDetailPanel } from './SensorDetailPanel'
import { useState } from 'react'

const SENSOR_LAYOUT = [
  { sensorId: 'sensor_01', readingType: 'temperature', position: [-2, 0.5, 1], min: 15, max: 45 },
  { sensorId: 'sensor_02', readingType: 'humidity',    position: [0, 0.5, -1],  min: 20, max: 90 },
  { sensorId: 'sensor_03', readingType: 'pressure',    position: [2, 0.5, 1],   min: 900, max: 1100 },
] as const

export function DigitalTwinView() {
  const [selectedSensor, setSelectedSensor] = useState<string | null>(null)

  return (
    <div className="twin-container" style={{ width: '100%', height: '70vh' }}>
      <Canvas shadows camera={{ position: [5, 5, 5], fov: 50 }}>
        <ambientLight intensity={0.3} />
        <directionalLight position={[5, 10, 5]} intensity={1} castShadow />
        <Environment preset="city" />

        <Grid
          args={[20, 20]}
          cellSize={1}
          cellColor="#6f6f6f"
          sectionColor="#9f9f9f"
          fadeDistance={25}
          position={[0, 0, 0]}
        />

        {SENSOR_LAYOUT.map((s) => (
          <SensorNode3D
            key={s.sensorId}
            sensorId={s.sensorId}
            readingType={s.readingType}
            position={s.position as [number, number, number]}
            minValue={s.min}
            maxValue={s.max}
            onClick={setSelectedSensor}
          />
        ))}

        <OrbitControls enableDamping dampingFactor={0.05} minDistance={3} maxDistance={20} />

        {/* Bloom post-processing makes glowing sensor nodes pop */}
        <EffectComposer>
          <Bloom luminanceThreshold={0.6} luminanceSmoothing={0.9} intensity={0.8} />
        </EffectComposer>
      </Canvas>

      {selectedSensor && (
        <SensorDetailPanel
          sensorId={selectedSensor}
          onClose={() => setSelectedSensor(null)}
        />
      )}
    </div>
  )
}
```

### 5.6.4 Specialized Animated Nodes (Fan Example)

```tsx
// components/twin/FanNode3D.tsx
import { useRef } from 'react'
import { useFrame } from '@react-three/fiber'
import * as THREE from 'three'
import { useSensorContext } from '../../context/SensorContext'

interface FanNode3DProps {
  sensorId: string
  position: [number, number, number]
  maxRPM: number
}

export function FanNode3D({ sensorId, position, maxRPM }: FanNode3DProps) {
  const fanRef = useRef<THREE.Group>(null!)
  const { sensorData } = useSensorContext()

  const rpm = sensorData[sensorId]?.['rpm']?.value ?? 0

  useFrame((_, delta) => {
    if (!fanRef.current) return
    const radsPerSec = (rpm / maxRPM) * Math.PI * 4
    fanRef.current.rotation.y += radsPerSec * delta
  })

  return (
    <group position={position}>
      <mesh>
        <cylinderGeometry args={[0.1, 0.1, 0.2, 16]} />
        <meshStandardMaterial color="#666" metalness={0.9} />
      </mesh>
      <group ref={fanRef}>
        {[0, 1, 2, 3].map((i) => (
          <mesh key={i} rotation={[0, (Math.PI / 2) * i, Math.PI / 6]}>
            <boxGeometry args={[0.6, 0.02, 0.15]} />
            <meshStandardMaterial color="#aaa" metalness={0.7} />
          </mesh>
        ))}
      </group>
    </group>
  )
}
```

### 5.6.5 Enhanced SensorContext for 3D + Anomaly

```tsx
// context/SensorContext.tsx
type ReadingData = {
  value: number
  unit: string
  timestamp: string
  status: 'normal' | 'warning' | 'critical'
  anomaly?: {
    score: number
    is_anomaly: boolean
    confidence: number
    contributing_features: string[]
  }
}

type SensorState = {
  [sensorId: string]: {
    [readingType: string]: ReadingData
  }
}

type Action =
  | { type: 'WS_FLUSH'; readings: Array<{
      sensor_id: string; reading_type: string; value: number;
      unit: string; timestamp: string; anomaly?: any
    }> }
  | { type: 'SELECT_SENSOR'; sensorId: string | null }

function classifyStatus(value: number, sensorId: string, readingType: string): 'normal' | 'warning' | 'critical' {
  // Placeholder — thresholds per sensor/reading_type would be loaded from config
  return 'normal'
}

function sensorReducer(state: SensorState, action: Action): SensorState {
  switch (action.type) {
    case 'WS_FLUSH': {
      const next = { ...state }
      for (const r of action.readings) {
        if (!next[r.sensor_id]) next[r.sensor_id] = {}
        next[r.sensor_id][r.reading_type] = {
          value: r.value,
          unit: r.unit,
          timestamp: r.timestamp,
          status: classifyStatus(r.value, r.sensor_id, r.reading_type),
          anomaly: r.anomaly,
        }
      }
      return next
    }
    default:
      return state
  }
}
```

### 5.6.6 WebSocket → 3D Scene Data Flow

```mermaid
sequenceDiagram
    participant WS as FastAPI WS /ws/live
    participant Hook as useWebSocket
    participant Ctx as SensorContext
    participant Node as SensorNode3D
    participant Frame as R3F useFrame (60fps)
    participant GPU as WebGL Renderer

    WS->>Hook: {"event":"flush_complete","readings":[...]}
    Hook->>Ctx: dispatch({ type: 'WS_FLUSH', readings })
    Ctx->>Node: Re-render with new props (value, status, anomaly)
    Note over Node: React re-render sets target color/speed/animation
    loop Every animation frame (~16ms)
        Frame->>Node: useFrame callback
        Node->>Node: Lerp color, rotation, glow toward target (delta-smoothed)
        Node->>GPU: Updated mesh properties
    end
```

### 5.6.7 Performance Guardrails

| Concern | Mitigation |
|---------|-----------|
| Too many draw calls (100+ sensors) | Use `<Instances>` from drei for batched instanced rendering |
| Memory on mobile/low-end | `<AdaptiveDpr>` + `<AdaptiveEvents>` from drei auto-downgrades |
| Large 3D models | Use `glTF` + Draco compression; lazy-load via `useGLTF.preload()` |
| React re-renders | Sensor nodes use `React.memo`; only re-render when their specific sensor data changes |
| Post-processing cost | Bloom only applied via `selective` mode on emissive meshes |

---

## 6. Error Handling & Edge Cases

| Scenario | Handling |
|----------|----------|
| MQTT broker unreachable at startup | Retry with exponential backoff (max 5 retries), then exit with error + health check reflects `mqtt_connected: false` |
| JSON parse failure on MQTT message | Log warning, skip message, do not crash |
| Invalid sensor_id in message | Validate against expected pattern, reject if malformed |
| InfluxDB write failure | Circuit Breaker transitions OPEN; data spills to disk; retry probe after 60s |
| Circuit OPEN extended outage | Disk spillover accumulates; FIFO drain on recovery |
| WebSocket client disconnects | Server cleans up connection; client auto-reconnects with backoff |
| API request for unknown sensor | Return `404` with descriptive message |
| Empty buffer on flush trigger | Skip write (no-op), log "nothing to flush" |
| Sensor stops publishing | Buffer holds last known value; after 2 missed flush cycles (10 min), health check alerts via `stale: true` flag |
| ML model crashes or is missing | `AnomalyScorer.score()` returns `is_anomaly: false` — ingestion always continues |
| Redis unavailable | `redis_client.set()` raises — caught and logged; ingestion continues; `/latest` falls back to InfluxDB |

---

## 7. Anomaly Detection Engine — Implementation Details

### 7.1 Feature Extraction

Each sensor reading is transformed into a feature vector using a **sliding window** of the last 20 readings for that sensor+reading_type combination.

```python
# anomaly/feature_extractor.py
import numpy as np
from collections import deque

class SensorFeatureExtractor:
    """Maintains per-sensor sliding windows and extracts statistical features."""

    def __init__(self, window_size: int = 20):
        self.window_size = window_size
        self.windows: dict[tuple[str, str], deque[float]] = {}

    def extract(self, sensor_id: str, reading_type: str, value: float) -> np.ndarray:
        key = (sensor_id, reading_type)
        if key not in self.windows:
            self.windows[key] = deque(maxlen=self.window_size)

        window = self.windows[key]
        window.append(value)
        values = np.array(window)

        if len(values) < 3:
            return np.zeros(6)

        return np.array([
            value,
            np.mean(values),
            np.std(values),
            value - values[-2] if len(values) > 1 else 0,
            value - np.min(values),
            np.max(values) - value,
        ])
```

### 7.2 Anomaly Scorer

```python
# anomaly/scorer.py
import joblib
import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class AnomalyScorer:
    def __init__(self, model_path: str = "/models/iforest.joblib", threshold: float = -0.5):
        self.threshold = threshold
        self.model = None
        self.model_version = "none"
        self._load_model(model_path)

    def _load_model(self, path: str):
        p = Path(path)
        if p.exists():
            self.model = joblib.load(p)
            self.model_version = p.stem
            logger.info(f"Loaded anomaly model: {self.model_version}")
        else:
            logger.warning(f"No model at {path} — anomaly detection disabled")

    def score(self, features: np.ndarray) -> dict:
        if self.model is None:
            return {"score": 0.0, "is_anomaly": False, "confidence": 0.0,
                    "contributing_features": [], "model_version": "disabled"}
        try:
            raw_score = float(self.model.decision_function(features.reshape(1, -1))[0])
            is_anomaly = raw_score < self.threshold
            confidence = min(abs(raw_score) / abs(self.threshold), 1.0)

            contributing = []
            if len(features) >= 4 and abs(features[3]) > 0.5:
                contributing.append("rate_of_change")

            return {
                "score": round(raw_score, 4),
                "is_anomaly": is_anomaly,
                "confidence": round(confidence, 4),
                "contributing_features": contributing,
                "model_version": self.model_version,
            }
        except Exception as e:
            logger.error(f"Anomaly scoring failed: {e}")
            return {"score": 0.0, "is_anomaly": False, "confidence": 0.0,
                    "contributing_features": [], "model_version": "error"}
```

### 7.3 Model Training Script (Offline)

```python
# scripts/train_anomaly_model.py
"""
Offline training script. Run against historical InfluxDB data to generate
the Isolation Forest model file.

Usage: python scripts/train_anomaly_model.py --hours 48 --output /models/iforest.joblib
"""
from sklearn.ensemble import IsolationForest
import joblib
import numpy as np

def train_model(historical_features: np.ndarray, contamination: float = 0.05):
    model = IsolationForest(
        n_estimators=100,
        contamination=contamination,
        max_samples='auto',
        random_state=42,
        n_jobs=-1
    )
    model.fit(historical_features)
    return model

if __name__ == "__main__":
    features = fetch_historical_features(hours=48)  # → np.ndarray (N, 6)
    model = train_model(features)
    joblib.dump(model, "/models/iforest.joblib")
    print(f"Model saved. Training samples: {len(features)}")
```

### 7.4 Redis Pub/Sub for Real-Time Anomaly Alerts

```python
# In FastAPI WebSocket handler — subscribe to anomaly channel
async def ws_live(websocket: WebSocket):
    await websocket.accept()
    pubsub = redis_client.pubsub()
    pubsub.subscribe("alerts:anomaly")

    async for message in pubsub.listen():
        if message["type"] == "message":
            await websocket.send_text(message["data"])
```

---

## 8. Circuit Breaker — Detailed Design

### 8.1 State Machine

```mermaid
stateDiagram-v2
    [*] --> Closed

    Closed --> Open : failure_count >= FAILURE_THRESHOLD (5)
    Closed --> Closed : write succeeds → reset failure_count

    Open --> HalfOpen : RECOVERY_TIMEOUT elapsed (60s)
    Open --> Open : all calls rejected (fail-fast, no DB contact)

    HalfOpen --> Closed : probe write succeeds → reset counters
    HalfOpen --> Open : probe write fails → restart recovery timer

    note right of Closed : Normal operation. All writes go to InfluxDB.
    note right of Open : DB assumed down. Writes buffered locally to disk. No DB contact attempted.
    note right of HalfOpen : Send ONE probe write. Success → CLOSED. Failure → OPEN for another 60s.
```

### 8.2 Disk Spillover & Recovery

```python
DISK_BUFFER_DIR = Path(os.getenv("DISK_BUFFER_DIR", "/data/buffer"))
DISK_BUFFER_DIR.mkdir(parents=True, exist_ok=True)

def _spill_to_disk(readings: list[dict]):
    """Write readings to a timestamped JSON file as durable overflow."""
    filename = DISK_BUFFER_DIR / f"batch_{int(time.time())}.json"
    with open(filename, "w") as f:
        json.dump(readings, f)
    log_info(f"Spilled {len(readings)} readings to {filename}")

def _drain_disk_buffer():
    """Replay disk-buffered batches in FIFO order after circuit closes."""
    files = sorted(DISK_BUFFER_DIR.glob("batch_*.json"))
    for f in files:
        try:
            with open(f) as fh:
                readings = json.load(fh)
            write_to_influxdb(readings)
            f.unlink()
            log_info(f"Drained disk batch {f.name}")
        except Exception as e:
            log_error(f"Disk drain failed on {f.name}: {e}")
            break
```

---

## 9. OpenTelemetry — Setup & Collector Configuration

### 9.1 Python Dependencies

```
opentelemetry-api==1.23.0
opentelemetry-sdk==1.23.0
opentelemetry-exporter-otlp-proto-grpc==1.23.0
opentelemetry-instrumentation-fastapi==0.44b0
opentelemetry-instrumentation-redis==0.44b0
```

### 9.2 OTel Collector Config (`config/otel-collector.yaml`)

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:
    timeout: 5s
    send_batch_size: 1024

exporters:
  jaeger:
    endpoint: jaeger:14250
    tls:
      insecure: true
  logging:
    loglevel: info

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [jaeger, logging]
```

### 9.3 Trace Anatomy: MQTT → Redis → InfluxDB → WebSocket

```
Trace: mqtt_pipeline (trace_id: abc123)
│
├── Span: mqtt_message_received          [0ms ─── 1ms]
│   ├── attribute: sensor_id = "sensor_01"
│   └── attribute: reading_type = "temperature"
│
├── Span: anomaly_scoring                [1ms ─── 2.5ms]
│   ├── attribute: model_version = "iforest_v1"
│   └── attribute: anomaly_score = -0.82
│
├── Span: redis_speed_layer_write        [2.5ms ─── 3ms]
│   └── attribute: redis_key = "latest:sensor_01:temperature"
│
├── Span: buffer_insert                  [3ms ─── 3.1ms]
│   └── attribute: buffer.total_sensors = 10
│
└── [... 300 seconds later, linked span ...]
    │
    ├── Span: influx_batch_flush         [0ms ─── 45ms]
    │   ├── attribute: circuit_breaker.state = "closed"
    │   └── attribute: batch.size = 10
    │
    └── Span: ws_broadcast               [45ms ─── 48ms]
        └── attribute: ws.connected_clients = 3
```

### 9.4 FastAPI OTel Instrumented Endpoint Example

```python
# services/api/main.py
from fastapi import FastAPI
from otel.setup import init_telemetry
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

app = FastAPI(title="IoT Telemetry API")
tracer = init_telemetry("iot-api-service")
FastAPIInstrumentor.instrument_app(app)  # Traces every HTTP request automatically

@app.get("/api/v1/sensors/{sensor_id}/data")
async def get_sensor_data(sensor_id: str, start: str = "-24h", end: str = "now"):
    with tracer.start_as_current_span("influx_query",
            attributes={"sensor.id": sensor_id, "query.start": start}) as span:
        data = query_influxdb(sensor_id, start, end)
        span.set_attribute("query.result_count", len(data))
        return {"sensor_id": sensor_id, "data": data}
```