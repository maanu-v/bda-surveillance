# spark_yolo_consumer.py
import cv2
import numpy as np
import base64
import io
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StringType, StructType, StructField
from ultralytics import YOLO

# ----------------------------
# Initialize Spark
# ----------------------------
spark = SparkSession.builder \
    .appName("YOLO-Spark-Consumer") \
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
detections = json_stream.withColumn("detections", detect_udf(col("data")))

# ----------------------------
# Output
# ----------------------------
query = detections.select("frame_id", "timestamp", "detections") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
