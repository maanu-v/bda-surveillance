from flask import Flask, render_template, Response, request, jsonify
import cv2
import numpy as np
from kafka import KafkaConsumer
import threading
import queue
import time
from datetime import datetime
import base64
import os
import glob
import json

app = Flask(__name__)

# --------------------------
# Configuration
# --------------------------
KAFKA_BROKER = "localhost:9092"
RAW_TOPIC = "cam1-frames"
DETECTION_TOPIC = "cam1-detections"
CONSUMER_GROUP = "web-ui-group"
ANNOTATED_FRAMES_DIR = "/home/asai/bda/bda-surveillance/detection_results"

# Global variables for frame management
raw_frame_queue = queue.Queue(maxsize=10)  # Buffer for raw frames
annotated_frame_queue = queue.Queue(maxsize=10)  # Buffer for annotated frames
latest_raw_frame = None
latest_annotated_frame = None
raw_frame_lock = threading.Lock()
annotated_frame_lock = threading.Lock()
consumer_running = False
stats = {
    'raw_frames_received': 0,
    'annotated_frames_received': 0,
    'raw_fps': 0,
    'annotated_fps': 0,
    'last_update': datetime.now(),
    'raw_queue_size': 0,
    'annotated_queue_size': 0,
    'consumer_status': 'Disconnected'
}

def kafka_raw_consumer_thread():
    """Background thread to consume raw frames from Kafka"""
    global latest_raw_frame, consumer_running, stats
    
    print("[INFO] Starting Kafka raw frames consumer thread...")
    
    try:
        consumer = KafkaConsumer(
            RAW_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=f"{CONSUMER_GROUP}_raw",
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: x,
            consumer_timeout_ms=1000
        )
        
        print("[INFO] Raw frames Kafka consumer connected successfully")
        
        frame_count = 0
        start_time = time.time()
        
        while consumer_running:
            try:
                message_pack = consumer.poll(timeout_ms=1000)
                
                if not message_pack:
                    continue
                
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        if not consumer_running:
                            break
                            
                        try:
                            nparr = np.frombuffer(message.value, np.uint8)
                            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                            
                            if frame is not None:
                                with raw_frame_lock:
                                    latest_raw_frame = frame.copy()
                                
                                try:
                                    raw_frame_queue.put_nowait(frame)
                                except queue.Full:
                                    try:
                                        raw_frame_queue.get_nowait()
                                        raw_frame_queue.put_nowait(frame)
                                    except queue.Empty:
                                        pass
                                
                                frame_count += 1
                                current_time = time.time()
                                elapsed = current_time - start_time
                                
                                if elapsed > 1.0:
                                    stats['raw_fps'] = frame_count / elapsed
                                    stats['raw_frames_received'] += frame_count
                                    stats['last_update'] = datetime.now()
                                    stats['raw_queue_size'] = raw_frame_queue.qsize()
                                    frame_count = 0
                                    start_time = current_time
                                
                        except Exception as e:
                            print(f"[ERROR] Failed to decode raw frame: {e}")
                            continue
                            
            except Exception as e:
                print(f"[ERROR] Raw consumer poll error: {e}")
                time.sleep(1)
                
    except Exception as e:
        print(f"[ERROR] Raw Kafka consumer failed: {e}")
        
    finally:
        try:
            consumer.close()
        except:
            pass
        print("[INFO] Raw Kafka consumer thread stopped")

def kafka_detection_consumer_thread():
    """Background thread to consume detection messages and create annotated frames"""
    global latest_annotated_frame, consumer_running, stats
    
    print("[INFO] Starting Kafka detection consumer thread...")
    
    try:
        # Use a simpler approach - consume detections and use latest raw frame
        detection_consumer = KafkaConsumer(
            DETECTION_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=f"{CONSUMER_GROUP}_detections",
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        
        print("[INFO] Detection consumer connected successfully")
        
        frame_count = 0
        start_time = time.time()
        
        while consumer_running:
            try:
                # Poll for detection messages
                detection_messages = detection_consumer.poll(timeout_ms=100)
                for topic_partition, messages in detection_messages.items():
                    for message in messages:
                        if not consumer_running:
                            break
                        try:
                            detection_data = message.value
                            detections = detection_data.get('detections', [])
                            
                            # Use the latest raw frame as base for annotation
                            current_frame = None
                            with raw_frame_lock:
                                if latest_raw_frame is not None:
                                    current_frame = latest_raw_frame.copy()
                            
                            if current_frame is not None:
                                # Create annotated frame
                                annotated_frame = draw_detections_on_frame(current_frame, detections)
                                
                                with annotated_frame_lock:
                                    latest_annotated_frame = annotated_frame.copy()
                                
                                try:
                                    annotated_frame_queue.put_nowait(annotated_frame)
                                except queue.Full:
                                    try:
                                        annotated_frame_queue.get_nowait()
                                        annotated_frame_queue.put_nowait(annotated_frame)
                                    except queue.Empty:
                                        pass
                                
                                frame_count += 1
                                current_time = time.time()
                                elapsed = current_time - start_time
                                
                                if elapsed > 1.0:
                                    stats['annotated_fps'] = frame_count / elapsed
                                    stats['annotated_frames_received'] += frame_count
                                    stats['annotated_queue_size'] = annotated_frame_queue.qsize()
                                    frame_count = 0
                                    start_time = current_time
                                
                        except Exception as e:
                            print(f"[ERROR] Failed to process detection for annotation: {e}")
                        
            except Exception as e:
                print(f"[ERROR] Detection consumer poll error: {e}")
                time.sleep(1)
                
    except Exception as e:
        print(f"[ERROR] Detection Kafka consumer failed: {e}")
        
    finally:
        try:
            detection_consumer.close()
        except:
            pass
        print("[INFO] Detection Kafka consumer thread stopped")

def draw_detections_on_frame(frame, detections):
    """Draw bounding boxes and labels on frame"""
    annotated_frame = frame.copy()
    
    for i, detection in enumerate(detections):
        bbox = detection['bbox']
        confidence = detection['confidence']
        class_name = detection.get('class_name', 'person')
        
        # Extract coordinates
        x1, y1, x2, y2 = map(int, bbox)
        
        # Draw bounding box
        cv2.rectangle(annotated_frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
        
        # Create label text
        label = f"{class_name}: {confidence:.3f}"
        
        # Calculate text size
        (text_width, text_height), baseline = cv2.getTextSize(
            label, cv2.FONT_HERSHEY_SIMPLEX, 0.6, 1
        )
        
        # Draw label background
        label_y = y1 - 10 if y1 - 10 > 10 else y1 + text_height + 10
        cv2.rectangle(
            annotated_frame,
            (x1, label_y - text_height - baseline),
            (x1 + text_width, label_y + baseline),
            (0, 255, 0),
            -1
        )
        
        # Draw label text
        cv2.putText(
            annotated_frame,
            label,
            (x1, label_y - baseline),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.6,
            (255, 255, 255),
            1
        )
        
    return annotated_frame

def generate_frames(feed_type='raw'):
    """Generator function to yield frames for streaming"""
    global latest_raw_frame, latest_annotated_frame
    
    while True:
        frame = None
        
        if feed_type == 'raw':
            with raw_frame_lock:
                if latest_raw_frame is not None:
                    frame = latest_raw_frame.copy()
        elif feed_type == 'annotated':
            with annotated_frame_lock:
                if latest_annotated_frame is not None:
                    frame = latest_annotated_frame.copy()
        
        if frame is None:
            # Create a placeholder frame if no frames available
            frame = np.zeros((480, 640, 3), dtype=np.uint8)
            if feed_type == 'raw':
                cv2.putText(frame, 'Waiting for raw video stream...', 
                           (50, 220), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
            else:
                cv2.putText(frame, 'Waiting for annotated stream...', 
                           (50, 200), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
                cv2.putText(frame, 'Make sure YOLO detection is running', 
                           (50, 240), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 0), 2)
        
        # Encode frame as JPEG
        ret, buffer = cv2.imencode('.jpg', frame)
        if ret:
            frame_bytes = buffer.tobytes()
            yield (b'--frame\r\n'
                   b'Content-Type: image/jpeg\r\n\r\n' + frame_bytes + b'\r\n')
        
        time.sleep(0.033)  # ~30 FPS max

@app.route('/')
def index():
    """Main page with video stream"""
    return render_template('index.html')

@app.route('/video_feed')
def video_feed():
    """Video streaming route - raw feed by default"""
    feed_type = request.args.get('type', 'raw')
    return Response(generate_frames(feed_type),
                    mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/raw_feed')
def raw_feed():
    """Raw video streaming route"""
    return Response(generate_frames('raw'),
                    mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/annotated_feed')
def annotated_feed():
    """Annotated video streaming route"""
    return Response(generate_frames('annotated'),
                    mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/stats')
def get_stats():
    """API endpoint for statistics"""
    return {
        'raw_frames_received': stats['raw_frames_received'],
        'annotated_frames_received': stats['annotated_frames_received'],
        'raw_fps': round(stats['raw_fps'], 2),
        'annotated_fps': round(stats['annotated_fps'], 2),
        'last_update': stats['last_update'].strftime('%H:%M:%S'),
        'raw_queue_size': stats['raw_queue_size'],
        'annotated_queue_size': stats['annotated_queue_size'],
        'consumer_status': stats['consumer_status']
    }

@app.route('/switch_feed', methods=['POST'])
def switch_feed():
    """API endpoint to switch feed type"""
    data = request.get_json()
    feed_type = data.get('type', 'raw')
    return jsonify({'status': 'success', 'feed_type': feed_type})

if __name__ == '__main__':
    # Start consumers in background threads
    consumer_running = True
    stats['consumer_status'] = 'Starting...'
    
    raw_consumer_thread = threading.Thread(target=kafka_raw_consumer_thread, daemon=True)
    detection_consumer_thread = threading.Thread(target=kafka_detection_consumer_thread, daemon=True)
    
    raw_consumer_thread.start()
    detection_consumer_thread.start()
    
    stats['consumer_status'] = 'Connected'
    
    print("[INFO] Starting Flask web server...")
    print("[INFO] Open http://localhost:5001 in your browser to view the video stream")
    
    try:
        app.run(host='0.0.0.0', port=5001, debug=False, threaded=True)
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down...")
        consumer_running = False
