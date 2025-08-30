import json
import cv2
import numpy as np
from kafka import KafkaConsumer
import os
from datetime import datetime
import threading
import queue

# --------------------------
# Configuration
# --------------------------
KAFKA_BROKER = "localhost:9092"
CAMERA_ID = "cam1"
FRAME_TOPIC = f"{CAMERA_ID}-frames"
DETECTION_TOPIC = f"{CAMERA_ID}-detections"
CONSUMER_GROUP = "yolo-analytics-group"

# Output settings
SAVE_ANNOTATED_FRAMES = True
OUTPUT_DIR = "detection_results"
DISPLAY_FRAMES = False  # Set to True if you have display capability

class YOLOConsumer:
    def __init__(self):
        self.setup_output_dir()
        self.setup_kafka()
        self.frame_queue = queue.Queue(maxsize=100)
        self.detection_queue = queue.Queue(maxsize=100)
        self.processed_count = 0
        self.detection_count = 0
        self.start_time = datetime.now()
        
    def setup_output_dir(self):
        """Create output directory for annotated frames"""
        if SAVE_ANNOTATED_FRAMES and not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            print(f"[INFO] Created output directory: {OUTPUT_DIR}")
            
    def setup_kafka(self):
        """Initialize Kafka consumers"""
        print(f"[INFO] Setting up Kafka consumers")
        print(f"[INFO] Frame topic: {FRAME_TOPIC}")
        print(f"[INFO] Detection topic: {DETECTION_TOPIC}")
        
        # Consumer for raw frames
        self.frame_consumer = KafkaConsumer(
            FRAME_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=f"{CONSUMER_GROUP}-frames",
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: x  # Keep raw bytes
        )
        
        # Consumer for detection results
        self.detection_consumer = KafkaConsumer(
            DETECTION_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=f"{CONSUMER_GROUP}-detections",
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print(f"[INFO] Kafka consumers initialized")
        
    def consume_frames(self):
        """Consumer thread for raw frames"""
        print(f"[INFO] Starting frame consumer thread")
        try:
            for message in self.frame_consumer:
                if not self.frame_queue.full():
                    self.frame_queue.put({
                        'data': message.value,
                        'offset': message.offset,
                        'partition': message.partition
                    })
        except Exception as e:
            print(f"[ERROR] Frame consumer error: {e}")
            
    def consume_detections(self):
        """Consumer thread for detection results"""
        print(f"[INFO] Starting detection consumer thread")
        try:
            for message in self.detection_consumer:
                if not self.detection_queue.full():
                    self.detection_queue.put(message.value)
        except Exception as e:
            print(f"[ERROR] Detection consumer error: {e}")
            
    def draw_detections(self, frame, detections):
        """Draw bounding boxes and labels on frame"""
        annotated_frame = frame.copy()
        
        for detection in detections:
            bbox = detection['bbox']
            confidence = detection['confidence']
            class_name = detection['class_name']
            
            # Extract coordinates
            x1, y1, x2, y2 = map(int, bbox)
            
            # Draw bounding box
            cv2.rectangle(annotated_frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
            
            # Draw label
            label = f"{class_name}: {confidence:.2f}"
            label_size = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)[0]
            cv2.rectangle(annotated_frame, (x1, y1 - label_size[1] - 10), 
                         (x1 + label_size[0], y1), (0, 255, 0), -1)
            cv2.putText(annotated_frame, label, (x1, y1 - 5), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 0), 1)
                       
        return annotated_frame
        
    def process_detection_results(self):
        """Process detection results and create annotated frames"""
        print(f"[INFO] Starting detection processing")
        
        frame_cache = {}  # Cache frames by frame_id
        
        try:
            while True:
                # Get frames from queue
                try:
                    frame_data = self.frame_queue.get(timeout=1)
                    # Decode frame
                    nparr = np.frombuffer(frame_data['data'], np.uint8)
                    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                    if frame is not None:
                        # Use offset as frame identifier
                        frame_cache[frame_data['offset']] = frame
                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"[ERROR] Frame processing error: {e}")
                    continue
                
                # Get detection results
                try:
                    detection_data = self.detection_queue.get(timeout=1)
                    self.process_single_detection(detection_data, frame_cache)
                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"[ERROR] Detection processing error: {e}")
                    continue
                    
                # Clean old frames from cache (keep last 100)
                if len(frame_cache) > 100:
                    oldest_key = min(frame_cache.keys())
                    del frame_cache[oldest_key]
                    
        except KeyboardInterrupt:
            print(f"\n[INFO] Processing interrupted by user")
            
    def process_single_detection(self, detection_data, frame_cache):
        """Process a single detection result"""
        frame_id = detection_data['frame_id']
        detections = detection_data['detections']
        timestamp = detection_data['timestamp']
        camera_id = detection_data['camera_id']
        
        self.processed_count += 1
        self.detection_count += len(detections)
        
        # Print detection info
        if self.processed_count % 20 == 0:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            rate = self.processed_count / elapsed if elapsed > 0 else 0
            print(f"[INFO] Processed {self.processed_count} frames, "
                  f"{self.detection_count} total detections, {rate:.2f} frames/sec")
                  
        if detections:
            print(f"[DETECTION] Frame {frame_id}: {len(detections)} persons detected")
            for i, det in enumerate(detections):
                bbox = det['bbox']
                conf = det['confidence']
                print(f"  Person {i+1}: bbox=({bbox[0]:.0f},{bbox[1]:.0f},{bbox[2]:.0f},{bbox[3]:.0f}), conf={conf:.3f}")
        
        # Try to find corresponding frame for annotation
        # Since we might not have exact frame matching, use the most recent frame
        if frame_cache:
            # Get the most recent frame
            latest_frame_key = max(frame_cache.keys())
            frame = frame_cache[latest_frame_key]
            
            # Create annotated frame
            if detections and SAVE_ANNOTATED_FRAMES:
                annotated_frame = self.draw_detections(frame, detections)
                
                # Save annotated frame
                filename = f"{OUTPUT_DIR}/annotated_frame_{self.processed_count:06d}.jpg"
                cv2.imwrite(filename, annotated_frame)
                
                if self.processed_count % 50 == 0:
                    print(f"[INFO] Saved annotated frame: {filename}")
                    
            # Display frame if enabled
            if DISPLAY_FRAMES:
                if detections:
                    display_frame = self.draw_detections(frame, detections)
                else:
                    display_frame = frame
                    
                cv2.imshow(f'{camera_id} - YOLO Detection', display_frame)
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    return False
                    
        return True
        
    def run(self):
        """Main consumer loop"""
        print(f"[INFO] Starting YOLO detection consumer")
        print(f"[INFO] Camera: {CAMERA_ID}")
        print(f"[INFO] Save annotated frames: {SAVE_ANNOTATED_FRAMES}")
        print(f"[INFO] Display frames: {DISPLAY_FRAMES}")
        
        # Start consumer threads
        frame_thread = threading.Thread(target=self.consume_frames, daemon=True)
        detection_thread = threading.Thread(target=self.consume_detections, daemon=True)
        
        frame_thread.start()
        detection_thread.start()
        
        # Start processing
        try:
            self.process_detection_results()
        finally:
            # Cleanup
            self.frame_consumer.close()
            self.detection_consumer.close()
            if DISPLAY_FRAMES:
                cv2.destroyAllWindows()
                
            elapsed = (datetime.now() - self.start_time).total_seconds()
            rate = self.processed_count / elapsed if elapsed > 0 else 0
            print(f"[FINAL] Processed {self.processed_count} frames with {self.detection_count} total detections")
            print(f"[FINAL] Processing rate: {rate:.2f} frames/sec")

def main():
    try:
        consumer = YOLOConsumer()
        consumer.run()
    except Exception as e:
        print(f"[ERROR] Consumer failed: {e}")
        raise

if __name__ == "__main__":
    main()
