#!/bin/bash

# Production Spark Submit Script for YOLO Detection
echo "=========================================="
echo "SUBMITTING PRODUCTION YOLO SPARK JOB"
echo "=========================================="

SPARK_MASTER="spark://spark-master:7077"
APP_NAME="YOLO-Production-Detection"
MAIN_FILE="/opt/spark-apps/yolo_spark_production.py"

# Check if containers are running
echo "[INFO] Checking Spark cluster status..."
if ! docker ps | grep -q "spark-master"; then
    echo "[ERROR] Spark master container not running!"
    echo "Please start your Docker Compose services first:"
    echo "  docker-compose up -d"
    exit 1
fi

if ! docker ps | grep -q "kafka1"; then
    echo "[ERROR] Kafka containers not running!"
    echo "Please start your Docker Compose services first:"
    echo "  docker-compose up -d"
    exit 1
fi

echo "[INFO] Docker containers are running"

# Submit the production Spark application
echo "[INFO] Submitting Spark job..."
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
    --master $SPARK_MASTER \
    --deploy-mode client \
    --name "$APP_NAME" \
    --driver-memory 1g \
    --executor-memory 2g \
    --executor-cores 2 \
    --num-executors 2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    --conf spark.executor.heartbeatInterval=30s \
    --conf spark.network.timeout=300s \
    --conf spark.sql.execution.arrow.pyspark.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    $MAIN_FILE

echo "=========================================="
echo "Spark job submission completed!"
echo "=========================================="
echo ""
echo "Monitor your job:"
echo "  - Spark UI: http://localhost:8081"
echo "  - Kafka UI: http://localhost:8080"
echo ""
echo "Check output topic: cam1-detections-spark"
echo "  docker exec kafka1 kafka-console-consumer --bootstrap-server localhost:9092 --topic cam1-detections-spark --from-beginning"
echo ""
