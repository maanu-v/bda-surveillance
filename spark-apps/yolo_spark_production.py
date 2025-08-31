#!/usr/bin/env python3
"""
Production-ready Spark Streaming YOLO Detection
Uses actual YOLO model with proper error handling and monitoring
"""

import os
import sys
import json
import base64
import numpy as np
from datetime import datetime
import logging

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka1:29092,kafka2:29093"
INPUT_TOPIC = "cam1-frames"  
OUTPUT_TOPIC = "cam1-detections-spark"
CHECKPOINT_LOCATION = "/opt/spark-data/checkpoints/yolo-detection"
CONFIDENCE_THRESHOLD = 0.5
PERSON_CLASS_ID = 0

# Schema for detection results
DETECTION_SCHEMA = StructType([
    StructField("camera_id", StringType(), True),
    StructField("frame_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("detections", ArrayType(StructType([
        StructField("bbox", ArrayType(DoubleType()), True),
        StructField("confidence", DoubleType(), True),
        StructField("class_id", IntegerType(), True),
        StructField("class_name", StringType(), True)
    ])), True),
    StructField("detection_count", IntegerType(), True),
    StructField("model", StringType(), True),
    StructField("confidence_threshold", DoubleType(), True),
    StructField("processing_node", StringType(), True),
    StructField("processing_time_ms", DoubleType(), True)
])

def detect_persons_with_yolo(frame_bytes):
    """
    YOLO person detection function for Spark workers
    """
    import time
    start_time = time.time()
    
    try:
        # Import YOLO (install if needed)
        try:
            import cv2
            from ultralytics import YOLO
        except ImportError:
            # Auto-install dependencies
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", 
                                 "ultralytics", "opencv-python-headless"])
            import cv2
            from ultralytics import YOLO
        
        # Load model (cached after first load)
        if not hasattr(detect_persons_with_yolo, 'model'):
            model_path = "/opt/spark-apps/models/yolo11n.pt"
            if os.path.exists(model_path):
                detect_persons_with_yolo.model = YOLO(model_path)
                print(f"[INFO] YOLO model loaded from {model_path}")
            else:
                # Download model if not exists
                detect_persons_with_yolo.model = YOLO("yolo11n.pt")
                print("[INFO] YOLO model downloaded and loaded")
        
        # Decode frame
        if isinstance(frame_bytes, str):
            frame_data = base64.b64decode(frame_bytes)
        else:
            frame_data = frame_bytes
            
        nparr = np.frombuffer(frame_data, np.uint8)
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if frame is None:
            raise ValueError("Failed to decode frame")
        
        # Run YOLO detection
        results = detect_persons_with_yolo.model(
            frame, 
            conf=CONFIDENCE_THRESHOLD, 
            classes=[PERSON_CLASS_ID],
            verbose=False
        )
        
        # Extract detections
        detections = []
        for result in results:
            boxes = result.boxes
            if boxes is not None:
                for box in boxes:
                    x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                    confidence = float(box.conf[0].cpu().numpy())
                    class_id = int(box.cls[0].cpu().numpy())
                    
                    detection = {
                        "bbox": [float(x1), float(y1), float(x2), float(y2)],
                        "confidence": confidence,
                        "class_id": class_id,
                        "class_name": "person"
                    }
                    detections.append(detection)
        
        processing_time = (time.time() - start_time) * 1000  # ms
        
        result = {
            "camera_id": "cam1",
            "frame_id": "",  # Will be filled by Spark
            "timestamp": datetime.now().isoformat(),
            "detections": detections,
            "detection_count": len(detections),
            "model": "yolo11n.pt",
            "confidence_threshold": CONFIDENCE_THRESHOLD,
            "processing_node": f"spark-worker-{os.getpid()}",
            "processing_time_ms": processing_time
        }
        
        return json.dumps(result)
        
    except Exception as e:
        processing_time = (time.time() - start_time) * 1000
        error_result = {
            "camera_id": "cam1",
            "frame_id": "",
            "timestamp": datetime.now().isoformat(),
            "detections": [],
            "detection_count": 0,
            "model": "yolo11n.pt",
            "confidence_threshold": CONFIDENCE_THRESHOLD,
            "processing_node": f"spark-worker-{os.getpid()}",
            "processing_time_ms": processing_time,
            "error": str(e)
        }
        print(f"[ERROR] YOLO detection failed: {e}")
        return json.dumps(error_result)

def create_spark_session():
    """Create optimized Spark session for YOLO processing"""
    return SparkSession.builder \
        .appName("YOLO-Production-Streaming") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .getOrCreate()

def main():
    """Main production Spark streaming application"""
    print("=" * 60)
    print("STARTING PRODUCTION YOLO DETECTION ON SPARK")
    print("=" * 60)
    print(f"Input topic: {INPUT_TOPIC}")
    print(f"Output topic: {OUTPUT_TOPIC}")
    print(f"Kafka servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Confidence threshold: {CONFIDENCE_THRESHOLD}")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Register UDF
    detect_udf = udf(detect_persons_with_yolo, StringType())
    
    try:
        # Read from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", INPUT_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 3) \
            .option("failOnDataLoss", "false") \
            .load()
        
        print("[INFO] Connected to Kafka input stream")
        
        # Add frame metadata and detect persons
        processed_df = kafka_df.select(
            col("key").cast("string").alias("frame_key"),
            col("timestamp").alias("kafka_timestamp"),
            col("offset"),
            col("partition"),
            detect_udf(col("value")).alias("detection_json")
        )
        
        # Parse detection results and add frame_id
        enriched_df = processed_df.select(
            col("frame_key"),
            col("kafka_timestamp"),
            col("offset"),
            col("partition"),
            from_json(col("detection_json"), DETECTION_SCHEMA).alias("detection"),
            col("detection_json")
        ).select(
            col("frame_key"),
            col("kafka_timestamp"),
            col("detection.detection_count"),
            col("detection.processing_time_ms"),
            col("detection.processing_node"),
            # Update frame_id in the JSON
            regexp_replace(
                col("detection_json"),
                '"frame_id":"[^"]*"',
                concat(lit('"frame_id":"'), col("frame_key"), lit('"'))
            ).alias("final_detection_json")
        )
        
        # Console output for monitoring
        console_query = enriched_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 5) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        # Write results to Kafka
        kafka_output_query = enriched_df.select(
            col("frame_key").alias("key"),
            col("final_detection_json").alias("value")
        ) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime='2 seconds') \
        .start()
        
        print("[INFO] Streaming queries started successfully")
        print("[INFO] YOLO detection running on Spark cluster...")
        print(f"[INFO] Results published to: {OUTPUT_TOPIC}")
        print("[INFO] Monitor Spark UI at: http://localhost:8081")
        print("[INFO] Press Ctrl+C to stop")
        
        # Wait for termination
        kafka_output_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n[INFO] Gracefully stopping streaming application...")
        
    except Exception as e:
        print(f"[ERROR] Streaming application failed: {e}")
        import traceback
        traceback.print_exc()
        raise
        
    finally:
        spark.stop()
        print("[INFO] Spark session stopped successfully")

if __name__ == "__main__":
    main()
