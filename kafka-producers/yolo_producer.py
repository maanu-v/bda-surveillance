import cv2
import json
import os
import tempfile
import time
from datetime import datetime
from hdfs import InsecureClient
from kafka import KafkaProducer
from ultralytics import YOLO
import numpy as np

# --------------------------
# Configuration
# --------------------------
HDFS_URL = "http://localhost:9870"
HDFS_PATH = "/surveillance/wildtrack/videos/cam1.mp4"
KAFKA_BROKER = "localhost:9092"
CAMERA_ID = "cam1"
FRAME_TOPIC = f"{CAMERA_ID}-frames"      # Raw frames
DETECTION_TOPIC = f"{CAMERA_ID}-detections"  # YOLO detection results

# YOLO Configuration
YOLO_MODEL = "yolo11n.pt"  # Nano model for speed
CONFIDENCE_THRESHOLD = 0.5
PERSON_CLASS_ID = 0  # COCO class ID for person

# Performance settings
FRAME_SKIP = 1  # Process every N frames (1 = all frames)
MAX_FRAME_SIZE = (640, 480)  # Resize for faster processing

class SurveillanceProducer:
    def __init__(self):
        self.setup_yolo()
        self.setup_kafka()
        self.setup_hdfs()
        self.frame_count = 0
        self.detection_count = 0
        self.start_time = time.time()
        
    def setup_yolo(self):
        """Initialize YOLO model for person detection"""
        print(f"[INFO] Loading YOLO model: {YOLO_MODEL}")
        self.yolo_model = YOLO(YOLO_MODEL)
        print(f"[INFO] YOLO model loaded successfully")
        
    def setup_kafka(self):
        """Initialize Kafka producers"""
        print(f"[INFO] Connecting to Kafka: {KAFKA_BROKER}")
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else v
        )
        print(f"[INFO] Kafka producer initialized")
        
    def setup_hdfs(self):
        """Initialize HDFS client and download video"""
        print(f"[INFO] Connecting to HDFS: {HDFS_URL}")
        self.hdfs_client = InsecureClient(HDFS_URL, user="hdfs")
        
        # Try to download from HDFS, fallback to local file
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp_file:
            try:
                self.hdfs_client.download(HDFS_PATH, tmp_file.name, overwrite=True)
                self.video_path = tmp_file.name
                print(f"[INFO] Downloaded {CAMERA_ID}.mp4 from HDFS → {self.video_path}")
            except Exception as e:
                print(f"[ERROR] Failed to download from HDFS: {e}")
                print(f"[INFO] Using local file instead...")
                self.video_path = f"/home/asai/bda/bda-surveillance/data/wildtrack/{CAMERA_ID}.mp4"
                if not os.path.exists(self.video_path):
                    raise FileNotFoundError(f"Local file not found: {self.video_path}")
                    
    def resize_frame(self, frame):
        """Resize frame for optimal processing speed"""
        h, w = frame.shape[:2]
        if w > MAX_FRAME_SIZE[0] or h > MAX_FRAME_SIZE[1]:
            scale = min(MAX_FRAME_SIZE[0]/w, MAX_FRAME_SIZE[1]/h)
            new_w, new_h = int(w * scale), int(h * scale)
            frame = cv2.resize(frame, (new_w, new_h))
        return frame
        
    def detect_persons(self, frame):
        """Run YOLO detection on frame and return person detections"""
        # Run YOLO inference
        results = self.yolo_model(frame, conf=CONFIDENCE_THRESHOLD, classes=[PERSON_CLASS_ID])
        
        detections = []
        for result in results:
            boxes = result.boxes
            if boxes is not None:
                for box in boxes:
                    # Extract bounding box coordinates
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
                    
        return detections
        
    def create_frame_message(self, frame, frame_id, timestamp):
        """Create structured message for frame data"""
        # Encode frame as JPEG
        success, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
        if not success:
            return None
            
        message = {
            "camera_id": CAMERA_ID,
            "frame_id": frame_id,
            "timestamp": timestamp,
            "frame_data": buffer.tobytes(),
            "frame_shape": frame.shape,
            "encoding": "jpeg"
        }
        return message
        
    def create_detection_message(self, detections, frame_id, timestamp, frame_shape):
        """Create structured message for detection results"""
        message = {
            "camera_id": CAMERA_ID,
            "frame_id": frame_id,
            "timestamp": timestamp,
            "frame_shape": frame_shape,
            "detections": detections,
            "detection_count": len(detections),
            "model": YOLO_MODEL,
            "confidence_threshold": CONFIDENCE_THRESHOLD
        }
        return message
        
    def process_video(self):
        """Main processing loop"""
        print(f"[INFO] Starting video processing for {CAMERA_ID}")
        print(f"[INFO] Frame topic: {FRAME_TOPIC}")
        print(f"[INFO] Detection topic: {DETECTION_TOPIC}")
        
        cap = cv2.VideoCapture(self.video_path)
        if not cap.isOpened():
            raise ValueError(f"Cannot open video file: {self.video_path}")
            
        fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        
        print(f"[INFO] Video info: {total_frames} frames @ {fps} FPS")
        
        try:
            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    break
                    
                self.frame_count += 1
                
                # Skip frames for performance if needed
                if self.frame_count % FRAME_SKIP != 0:
                    continue
                    
                # Current timestamp
                timestamp = datetime.now().isoformat()
                
                # Resize frame for processing
                processed_frame = self.resize_frame(frame)
                
                # Run YOLO detection
                detections = self.detect_persons(processed_frame)
                
                # Send frame data to Kafka
                frame_message = self.create_frame_message(frame, self.frame_count, timestamp)
                if frame_message:
                    # Send frame data as raw bytes
                    self.producer.send(FRAME_TOPIC, frame_message["frame_data"])
                
                # Send detection results to Kafka
                detection_message = self.create_detection_message(
                    detections, self.frame_count, timestamp, processed_frame.shape
                )
                self.producer.send(DETECTION_TOPIC, detection_message)
                
                if detections:
                    self.detection_count += len(detections)
                
                # Progress reporting
                if self.frame_count % 50 == 0:
                    elapsed = time.time() - self.start_time
                    fps_actual = self.frame_count / elapsed
                    print(f"[INFO] Frame {self.frame_count}: {len(detections)} persons detected, "
                          f"{self.detection_count} total detections, {fps_actual:.2f} FPS")
                    
        except KeyboardInterrupt:
            print(f"\n[INFO] Interrupted by user")
            
        finally:
            cap.release()
            self.producer.flush()
            self.producer.close()
            
            elapsed = time.time() - self.start_time
            fps_actual = self.frame_count / elapsed if elapsed > 0 else 0
            print(f"[FINAL] Processed {self.frame_count} frames with {self.detection_count} total detections")
            print(f"[FINAL] Processing rate: {fps_actual:.2f} FPS")

def main():
    try:
        producer = SurveillanceProducer()
        producer.process_video()
    except Exception as e:
        print(f"[ERROR] Producer failed: {e}")
        raise

if __name__ == "__main__":
    main()
