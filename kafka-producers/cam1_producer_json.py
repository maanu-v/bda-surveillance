import cv2
import os
import time
import json
import base64
import tempfile
from datetime import datetime
from hdfs import InsecureClient
from kafka import KafkaProducer

# --------------------------
# HDFS & Kafka Config
# --------------------------
HDFS_URL = "http://localhost:9870"   # Use localhost when running from host
HDFS_PATH = "/surveillance/wildtrack/videos/cam1.mp4"
KAFKA_BROKERS = ["localhost:9092", "localhost:9093"]  # Use localhost when running from host
TOPIC = "cam1_frames"

# --------------------------
# Step 1: Download MP4 from HDFS (to temporary local file)
# --------------------------
local_video_path = None
try:
    hdfs_client = InsecureClient(HDFS_URL, user="hdfs")
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp_file:
        hdfs_client.download(HDFS_PATH, tmp_file.name, overwrite=True)
        local_video_path = tmp_file.name
        print(f"[INFO] Downloaded cam1.mp4 from HDFS → {local_video_path}")
except Exception as e:
    print(f"[ERROR] Failed to download from HDFS: {e}")
    print("[INFO] Trying to use local file instead...")
    local_video_path = "data/wildtrack/cam1.mp4"

# If still no file, fall back to webcam
use_webcam = False
if not os.path.exists(local_video_path):
    print(f"[WARN] Local video not found, using webcam instead")
    use_webcam = True

# --------------------------
# Step 2: Init Kafka Producer
# --------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON serialization
)

# --------------------------
# Step 3: Capture & Stream Frames
# --------------------------
if use_webcam:
    cap = cv2.VideoCapture(0)  # webcam
else:
    cap = cv2.VideoCapture(local_video_path)  # video file

frame_id = 0
print(f"[INFO] Streaming frames to Kafka topic '{TOPIC}'...")

while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        print("[INFO] End of stream")
        break

    # Encode frame as JPEG
    success, buffer = cv2.imencode(".jpg", frame)
    if not success:
        continue

    # Encode to base64 for JSON
    frame_base64 = base64.b64encode(buffer).decode('utf-8')
    
    # Create JSON message matching the expected schema
    timestamp_str = datetime.now().strftime("%Y-%m-%d-%H")  # Hour-level partitioning
    message = {
        "frame_id": str(frame_id),
        "timestamp": timestamp_str,
        "data": frame_base64
    }

    # Send JSON message to Kafka
    producer.send(TOPIC, message)
    frame_id += 1
    print(f"[FRAME {frame_id}] Sent JSON message ({len(frame_base64)} chars)")

    # Delay to simulate ~10 FPS camera
    time.sleep(0.1)

cap.release()
producer.flush()
print(f"[DONE] Finished sending {frame_id} frames to Kafka")
