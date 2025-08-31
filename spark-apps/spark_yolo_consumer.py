# spark_yolo_consumer.py
import cv2
import numpy as np
import base64
import io
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField
from ultralytics import YOLO

# ----------------------------
# Initialize Spark
# ----------------------------
spark = SparkSession.builder \
    .appName("YOLO-Spark-Consumer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Load YOLO model
# ----------------------------
yolo_model = YOLO("yolov8n.pt")   # you can use yolov8s.pt or yolov12 when available

# ----------------------------
# Schema for Kafka messages
# ----------------------------
schema = StructType([
    StructField("frame_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("data", StringType(), True),  # base64 encoded frame
])

# ----------------------------
# UDF: Decode + Run YOLO
# ----------------------------
def detect_objects(base64_img):
    try:
        img_bytes = base64.b64decode(base64_img)
        np_arr = np.frombuffer(img_bytes, np.uint8)
        frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

        results = yolo_model.predict(frame, conf=0.4, verbose=False)
        detections = []
        for box in results[0].boxes:
            cls = yolo_model.names[int(box.cls)]
            conf = float(box.conf)
            detections.append(f"{cls}:{conf:.2f}")
        return ", ".join(detections) if detections else "No objects"
    except Exception as e:
        return f"Error: {str(e)}"

detect_udf = udf(detect_objects, StringType())

# ----------------------------
# Kafka Stream
# ----------------------------
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:29092,kafka2:29093") \
    .option("subscribe", "cam1_frames") \
    .option("startingOffsets", "latest") \
    .load()

# Decode the Kafka value (JSON with base64 image)
json_stream = raw_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Apply YOLO detection
detections = json_stream.withColumn("detections", detect_udf(col("data"))) \
    .withColumn("processed_timestamp", current_timestamp())

# ----------------------------
# Output to Console
# ----------------------------
console_query = detections.select("frame_id", "timestamp", "detections") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

# ----------------------------
# Store Detection Results as Parquet (Console only for now due to permissions)
# Note: In production, you would set up proper HDFS/S3 permissions
# ----------------------------
# detections_query = detections.select("frame_id", "timestamp", "detections", "processed_timestamp") \
#     .writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", "/opt/spark-data/detections/cam1") \
#     .option("checkpointLocation", "/opt/spark-data/checkpoints/detections") \
#     .trigger(processingTime="10 seconds") \
#     .start()

# ----------------------------
# Store Raw Frames (Console only for now due to permissions)
# ----------------------------
# raw_frames_query = json_stream.select("frame_id", "timestamp", "data") \
#     .withColumn("stored_timestamp", current_timestamp()) \
#     .writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", "/opt/spark-data/raw_frames/cam1") \
#     .option("checkpointLocation", "/opt/spark-data/checkpoints/raw_frames") \
#     .trigger(processingTime="10 seconds") \
#     .start()

# ----------------------------
# Wait for all streams to finish
# ----------------------------
console_query.awaitTermination()
