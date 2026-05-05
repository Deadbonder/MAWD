# IoT Telemetry Pipeline & Analytical Dashboard — Product Requirements Document

**Version:** 1.0
**Date:** 2026-05-04
**Status:** Draft

---

## 1. Objective

Build a near-real-time IoT telemetry pipeline that ingests continuous sensor streams from 10+ physical devices via MQTT, buffers readings in memory, and persists aggregated 5-minute interval data to a time-series database. A FastAPI backend will serve this data to a React analytical dashboard, giving operators visibility into sensor health, trends, and anomalies across the deployed sensor network.

---

## 2. Scope

### In-Scope

- MQTT ingestion service subscribing to an external broker (`mqtt.geXXXXX:1XXX`, topic `aeXXXXXX/#`)
- In-memory buffering of the **latest** payload per sensor, flushed to the database every **300 seconds** (5 minutes) via APScheduler
- Time-series database (InfluxDB or TimescaleDB) for persistent sensor readings
- FastAPI REST API exposing historical sensor data and real-time KPIs
- WebSocket endpoint for live dashboard updates
- React + Tailwind CSS dashboard with KPI ribbon and time-series chart views
- Interactive 3D Digital Twin view (React Three Fiber) with real-time sensor visualization driven by WebSocket data from the Redis Speed Layer
- Full Docker Compose deployment (all services containerized)

### Out-of-Scope

- Sensor firmware or hardware changes
- Alerting / notification systems (email, SMS, Slack)
- Multi-tenant access control (single-org deployment)
- Mobile native apps (web dashboard only)
- Historical data export or ETL pipelines
- Command-and-control to sensors (write path to devices)

---

## 3. Functional Requirements

### FR-1: MQTT Subscription & In-Memory Buffering

- The Python ingestion service shall connect to the external MQTT broker at `mqtt.geXXXXX:1XXX`
- It shall subscribe to topic pattern `aeXXXXXX/#` (wildcard to capture all sensor sub-topics)
- For each incoming message, the service shall extract the `sensor_id` from the topic and store the **latest** payload in an in-memory dictionary keyed by `sensor_id`
- **Critical:** The in-memory buffer shall hold **only the most recent payload per sensor** — previous payloads are discarded on receipt of a new one. This minimizes memory footprint regardless of message frequency.

### FR-2: 5-Minute Scheduled Flush to Database

- A background scheduler (APScheduler, `IntervalTrigger`) shall fire every **300 seconds** (5 minutes)
- On each trigger, the ingestion service shall write **all** buffered sensor readings to the time-series database in a single batch insert
- After a successful flush, the in-memory buffer shall be **cleared** to prevent double-counting on the next flush cycle
- If a flush fails, the buffer contents shall be **retained** and retried on the next scheduled interval (to prevent data loss)
- The flush timestamp shall be recorded as the `processed_at` field in the database (not the sensor's own `timestamp`, which reflects when the reading was taken at the device)

### FR-3: Time-Series Database Persistence

- All sensor readings shall be stored with:
  - `sensor_id` (string, indexed)
  - `timestamp` (UTC, from sensor payload — the time the reading was taken)
  - `processed_at` (UTC, server-side — time of flush to DB)
  - `value` (float)
  - `unit` (string)
  - Additional fields as defined in the payload schema (see LLD)
- The database shall support time-range queries (e.g., "last 24 hours of sensor X")

### FR-4: FastAPI REST API

- `GET /api/v1/sensors` — list all registered sensor IDs
- `GET /api/v1/sensors/{sensor_id}/data?start=&end=&interval=` — retrieve historical readings
- `GET /api/v1/sensors/{sensor_id}/latest` — most recent reading for a sensor
- `GET /api/v1/kpis` — aggregate KPIs (min, max, avg, count per sensor over configurable window)
- All endpoints return JSON; pagination via `limit` / `offset`

### FR-5: Redis Speed Layer

- The system shall provide **sub-second access** to the most recent sensor reading via a high-speed caching layer (Redis)
- The Python ingestion service shall write every incoming MQTT message to Redis as a `latest:<sensor_id>:<reading_type>` key, overwriting the previous value for that sensor+type combination
- The FastAPI `/latest` endpoint shall read directly from Redis to serve real-time latest-value queries without hitting the time-series database
- Redis shall act as a "speed layer" complementing the InfluxDB "batch layer"; data is still persisted to InfluxDB every 5 minutes as the source of truth

### FR-6: WebSocket Real-Time Updates

- `WS /ws/live` — push latest sensor readings to connected clients as soon as they are flushed to the database (not on every MQTT message — respecting the 5-minute cadence)
- Clients receive a JSON payload per sensor per flush cycle

### FR-7: Analytical Dashboard

- **KPI Ribbon:** Display latest value, min, max, and trend arrow for each sensor in a horizontal scrolling strip
- **Chart View:** Time-series line charts with configurable time range (1h, 6h, 24h, 7d)
- **3D Digital Twin View:** Interactive 3D scene rendering all active sensors as spatial nodes; real-time visual reactions to sensor data (color, rotation, glow)
- **Responsive layout** built with React + Tailwind CSS (Vite)

### FR-8: Interactive 3D Digital Twin View

- The dashboard shall provide an optional **3D Digital Twin** view rendering all active sensors as interactive 3D nodes in a spatial scene
- Each sensor node shall visually react to its latest reading in real-time:
  - **Temperature sensors:** Node color interpolates from blue (cold) → green → red (hot) based on the sensor's value relative to its min/max KPI range
  - **Fan/motor sensors:** A 3D fan model rotates at a speed proportional to the reported RPM value
  - **Pressure sensors:** A gauge needle rotates proportionally to the value
- Sensor nodes shall be clickable; clicking opens a detail panel showing the same historical chart from `<ChartView />`
- The 3D scene shall use `OrbitControls` for pan/zoom/rotate navigation
- Real-time data shall flow from the existing WebSocket (`/ws/live`) through React context into React Three Fiber component props — no separate data channel

### FR-9: Edge AI Anomaly Detection

- The system shall perform inline anomaly detection on every incoming MQTT sensor reading before it reaches Redis or InfluxDB
- An Isolation Forest model shall score each reading in <2ms within the Python ingestion process (not a separate microservice)
- Anomalous readings shall be enriched with `anomaly.score`, `anomaly.is_anomaly`, and `anomaly.confidence` fields before persistence
- Anomaly alerts shall be published via Redis pub/sub and forwarded immediately to connected WebSocket clients as `anomaly_detected` events

### FR-10: Circuit Breaker & Disk Spillover

- The Python ingestion service shall implement a three-state Circuit Breaker (CLOSED / OPEN / HALF_OPEN) around the InfluxDB write path
- When the circuit is OPEN (InfluxDB unreachable), readings shall spill to durable disk storage (JSON files) to prevent data loss during extended outages
- The circuit breaker shall automatically transition to HALF_OPEN after a configurable recovery timeout (default 60s) to probe for DB recovery
- The health endpoint shall expose circuit breaker state, failure count, and disk buffer file count for observability

---

## 4. Non-Functional Requirements

### NFR-1: Latency

- MQTT-to-database flush latency: ≤ 10 seconds after the scheduler fires
- API response time for `/latest` and `/kpis` endpoints: ≤ 200 ms (p95)
- WebSocket push latency: ≤ 500 ms after database write

### NFR-2: Uptime & Availability

- Ingestion service: 99.5% uptime (monitored via health check endpoint)
- Database: persistent storage; no in-memory data loss on container restart except unflushed buffer (acceptable per design)
- API service: stateless — horizontally scalable behind a load balancer

### NFR-3: Data Integrity

- No duplicate writes: the buffer-overwrite strategy ensures only the latest reading per sensor per interval is stored
- Buffer retention on flush failure guarantees at-least-once delivery

### NFR-4: Scalability

- Support for **10+ sensors** as specified; architecture should scale to 100 sensors without redesign
- Ingestion service is single-threaded (Python GIL) but handles the expected load given the 5-minute flush cadence

### NFR-5: Observability

- Health check endpoint: `GET /health` returning `{"status": "ok", "buffer_size": N}`
- Structured JSON logging for all service events
- OpenTelemetry distributed tracing across all services (MQTT → Redis → InfluxDB → WebSocket); spans exported via OTLP to Jaeger UI
- Trace context propagated from MQTT message receipt through anomaly scoring, Redis write, buffer insert, and batch flush

### NFR-6: Resilience

- Circuit breaker pattern around InfluxDB writes: CLOSED (normal) → OPEN (fail-fast, no DB contact) → HALF_OPEN (probe)
- Disk spillover for unbounded retention during extended InfluxDB outages; FIFO drain on recovery
- Bounded in-memory buffer with per-sensor size guard (max 10,000 entries per sensor) to prevent OOM
- Anomaly scoring failures are non-blocking: ingestion always continues even if the ML model crashes

---

## 5. User Personas

### Persona 1: Plant/Facility Operator

- **Goal:** Monitor sensor health at a glance and spot anomalies before they become failures
- **Key interactions:** View KPI ribbon for live values, drill into a specific sensor's 24-hour chart
- **Technical comfort:** Low — needs a clean, color-coded UI with minimal complexity

### Persona 2: Data Analyst / Engineer

- **Goal:** Query historical sensor data, compare performance across time ranges, export datasets
- **Key interactions:** Use time-range selectors, toggle between sensors on overlay charts
- **Technical comfort:** Medium — comfortable with time-range parameters on API calls

### Persona 3: DevOps / Infrastructure Engineer

- **Goal:** Monitor pipeline health, debug connectivity issues, ensure data freshness
- **Key interactions:** Observe the health endpoint, check container logs, verify MQTT subscription status
- **Technical comfort:** High — needs structured logs and clear error messages

---

## 6. Assumptions & Constraints

- The external MQTT broker is **already deployed** and accessible from the Docker network
- Sensors publish continuously; message frequency may vary (unknown publish rate per sensor)
- The 5-minute flush interval is a **hard requirement** — no alternative cadence shall be considered without explicit scope change
- All timestamps stored in UTC
- Sensor firmware cannot be modified to batch-send on a 5-minute schedule — the buffering logic is server-side