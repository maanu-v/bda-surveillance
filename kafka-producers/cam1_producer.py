import cv2
import os
import tempfile
from hdfs import InsecureClient
from kafka import KafkaProducer

# --------------------------
# HDFS & Kafka Config
# --------------------------
HDFS_URL = "http://localhost:9870"   # WebHDFS endpoint of your namenode
HDFS_PATH = "/surveillance/wildtrack/videos/cam1.mp4"
KAFKA_BROKER = "localhost:9092"
TOPIC = "video-frames"

# --------------------------
# Step 1: Download MP4 from HDFS (temporary local file)
# --------------------------
# Configure HDFS client to handle Docker networking
hdfs_client = InsecureClient(HDFS_URL, user="hdfs")
with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp_file:
    try:
        hdfs_client.download(HDFS_PATH, tmp_file.name, overwrite=True)
        local_video_path = tmp_file.name
        print(f"[INFO] Downloaded cam1.mp4 from HDFS → {local_video_path}")
    except Exception as e:
        print(f"[ERROR] Failed to download from HDFS: {e}")
        print("[INFO] Trying to use local file instead...")
        # Fallback to local file if HDFS fails
        local_video_path = "/home/asai/bda/bda-surveillance/data/wildtrack/cam1.mp4"
        if not os.path.exists(local_video_path):
            print(f"[ERROR] Local file not found: {local_video_path}")
            exit(1)

# --------------------------
# Step 2: Init Kafka Producer
# --------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: v  # keep raw bytes
)

# --------------------------
# Step 3: Extract frames & push to Kafka
# --------------------------
cap = cv2.VideoCapture(local_video_path)
frame_id = 0

while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break

    # Encode frame as JPEG
    success, buffer = cv2.imencode(".jpg", frame)
    if not success:
        continue

    # Send frame bytes to Kafka
    producer.send(TOPIC, buffer.tobytes())
    frame_id += 1

    if frame_id % 50 == 0:
        print(f"[INFO] Sent {frame_id} frames to Kafka topic '{TOPIC}'")

cap.release()
producer.flush()
print(f"[DONE] Finished sending {frame_id} frames from cam1.mp4 to Kafka")
