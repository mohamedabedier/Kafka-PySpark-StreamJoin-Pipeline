#!/bin/bash

echo "=== [1/5] Checking Hadoop HDFS Status ==="
if hdfs dfsadmin -report &>/dev/null; then
    echo "[OK] HDFS is running."
else
    echo "[ERROR] HDFS is down! Please run start-dfs.sh first."
    exit 1
fi

echo "=== [2/5] Checking Kafka Topic ==="
kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -q "stream_orders"
if [ $? -eq 0 ]; then
    echo "[OK] Kafka topic 'stream_orders' exists."
else
    echo "[INFO] Creating Kafka topics..."
    kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic stream_orders
    kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic stream_payments
fi

echo "=== [3/5] Setting up HDFS Directories ==="
hdfs dfs -mkdir -p /user/student/streaming2/final/data/
hdfs dfs -mkdir -p /user/student/streaming2/finaldata/checkpoint/
echo "[OK] Directories created."

echo "=== [4/5] Creating Hive External Table ==="
hive -f ../hive/create_table.sql
echo "[OK] Hive table ready."

echo "=== [5/5] Launching Producer & Spark Streaming ==="
cd ../producer
nohup python3 generate_orders.py > ../producer.log 2>&1 &
echo $! > ../producer.pid
echo "[OK] Producer started (PID: $(cat ../producer.pid))"

cd ../spark
nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 spark_streaming.py > ../spark.log 2>&1 &
echo $! > ../spark.pid
echo "[OK] Spark Streaming started (PID: $(cat ../spark.pid))"

echo "=== Pipeline Deployed Successfully! ==="
