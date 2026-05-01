#!/bin/bash
set -e

echo "Starting application (Docker Mode)..."

# Datahub 已拆分为独立微服务，不再在 backend 中启动
# echo "Starting datahub scheduled task..."
# python -m app.lib.datahub --scheduled &
# DATAHUB_PID=$!

# Start Flask application
echo "Starting Flask application..."
python main.py &
FLASK_PID=$!

# Function to handle shutdown
shutdown() {
    echo "Shutting down..."
    # if [ -n "$DATAHUB_PID" ]; then
    #     kill $DATAHUB_PID 2>/dev/null || true
    # fi
    if [ -n "$FLASK_PID" ]; then
        kill $FLASK_PID 2>/dev/null || true
    fi
    wait
    echo "Shutdown complete"
    exit 0
}

# Trap signals
trap shutdown SIGTERM SIGINT

# Wait for any process to exit
# wait $DATAHUB_PID $FLASK_PID
wait $FLASK_PID

# If any process exits, shutdown all
shutdown
