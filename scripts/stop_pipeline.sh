#!/bin/bash

echo "=== Gracefully Stopping ShopPulse Pipeline ==="

if [ -f "../spark.pid" ]; then
    SPARK_PID=$(cat ../spark.pid)
    kill -15 $SPARK_PID 2>/dev/null
    echo "[OK] Stopped Spark Streaming (PID: $SPARK_PID)"
    rm ../spark.pid
else
    echo "[INFO] Spark PID file not found."
fi

if [ -f "../producer.pid" ]; then
    PRODUCER_PID=$(cat ../producer.pid)
    kill -15 $PRODUCER_PID 2>/dev/null
    echo "[OK] Stopped Python Producer (PID: $PRODUCER_PID)"
    rm ../producer.pid
else
    echo "[INFO] Producer PID file not found."
fi

echo "=== Shutdown Complete. HDFS Data and Checkpoints are safely preserved! ==="
