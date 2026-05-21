#!/bin/bash

# ====================================================================
# monitor.sh - Monitors Kafka consumer process and auto-restarts if down
# ====================================================================

CONSUMER_SCRIPT="kafka_consumer.py"
LOG_FILE="monitor.log"
CHECK_INTERVAL=30  # seconds

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Function to log with timestamp
log_event() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

# Function to check if process is running
is_running() {
    # Check if any python process is running the kafka_consumer.py
    pgrep -f "python.*$CONSUMER_SCRIPT" > /dev/null 2>&1
    return $?
}

# ====================================================================
# Main monitoring loop
# ====================================================================

echo "=============================================="
echo "Kafka Consumer Monitor"
echo "=============================================="
echo "Checking every ${CHECK_INTERVAL} seconds..."
echo "Log file: $LOG_FILE"
echo "Press Ctrl+C to stop"
echo "=============================================="

log_event "INFO: Monitor started"

while true; do
    if is_running; then
        echo -e "[$(date '+%H:%M:%S')] ${GREEN}[OK]${NC} Consumer is running"
    else
        echo -e "[$(date '+%H:%M:%S')] ${RED}[WARN]${NC} Consumer not running - restarting..."

        # Log the restart event
        log_event "WARN: Consumer was not running - restarting..."

        # Start the consumer in background
        python3 "$CONSUMER_SCRIPT" &
        sleep 2  # Give it time to start

        # Verify it started
        if is_running; then
            log_event "INFO: Consumer restarted successfully (PID: $!)"
            echo -e "[$(date '+%H:%M:%S')] ${GREEN}[OK]${NC} Consumer restarted (PID: $!)"
        else
            log_event "ERROR: Failed to restart consumer!"
            echo -e "[$(date '+%H:%M:%S')] ${RED}[ERROR]${NC} Failed to restart consumer!"
        fi
    fi

    sleep "$CHECK_INTERVAL"
done