import os
import io
import cv2
import torch
import base64
import json
import numpy as np

from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import BinaryType, StringType

# --------------------------
# Kafka Config
# --------------------------
KAFKA_BROKER = "localhost:9092"
FRAMES_TOPIC = "video-frames"
DETECTIONS_TOPIC = "video-detections"

# --------------------------
# Init Spark
# --------------------------
spark = (
    SparkSession.builder.appName("YOLO-Spark-Stream")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------
# Load YOLO model (torch hub or ultralytics)
# --------------------------
# You can replace with yolov8n if installed from ultralytics
# pip install ultralytics
from ultralytics import YOLO
yolo_model = YOLO("yolo11n.pt")  # lightweight model

# --------------------------
# Kafka Producer for detections
# --------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# --------------------------
# UDF: Run YOLO on each frame
# --------------------------
def run_yolo_on_frame(frame_bytes):
    try:
        # Decode bytes → numpy image
        img_array = np.frombuffer(frame_bytes, np.uint8)
        frame = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        if frame is None:
            return None

        # Run YOLO inference
        results = yolo_model(frame, verbose=False)

        detections = []
        for r in results:
            for box in r.boxes:
                detections.append({
                    "class": int(box.cls.cpu().numpy()[0]),
                    "confidence": float(box.conf.cpu().numpy()[0]),
                    "bbox": box.xyxy.cpu().numpy()[0].tolist(),  # [x1, y1, x2, y2]
                })

        if len(detections) > 0:
            # Publish detections to Kafka
            producer.send(DETECTIONS_TOPIC, {
                "detections": detections
            })

        return json.dumps({"num_detections": len(detections)})
    except Exception as e:
        return json.dumps({"error": str(e)})

# --------------------------
# Read stream from Kafka
# --------------------------
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", FRAMES_TOPIC)
    .load()
)

# value column is binary (the JPEG bytes)
frames_df = df.select(col("value").cast(BinaryType()).alias("frame"))

# Apply YOLO inference per frame (mapPartitions is better, but UDF easier demo)
from pyspark.sql.functions import udf

yolo_udf = udf(run_yolo_on_frame, StringType())
detections_df = frames_df.withColumn("yolo_result", yolo_udf(col("frame")))

# --------------------------
# Sink: For now just print results
# --------------------------
query = (
    detections_df.writeStream.outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
