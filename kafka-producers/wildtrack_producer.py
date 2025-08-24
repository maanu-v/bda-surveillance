"""
Wildtrack Dataset Kafka Producer (HDFS-enabled)
Simulates real-time streaming from 7 cameras using Wildtrack dataset stored in HDFS.
"""

import cv2
import json
import time
import argparse
import subprocess
from kafka import KafkaProducer
from pathlib import Path
import base64
import logging
import tempfile
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WildtrackKafkaProducer:
    def __init__(self, kafka_servers='localhost:9092', use_hdfs=True, hdfs_base_path='/surveillance/wildtrack'):
        """
        Initialize Wildtrack Kafka Producer with HDFS support
        
        Args:
            kafka_servers: Kafka bootstrap servers
            use_hdfs: Whether to use HDFS or local filesystem
            hdfs_base_path: Base path in HDFS for Wildtrack data
        """
        self.kafka_servers = kafka_servers
        self.use_hdfs = use_hdfs
        self.hdfs_base_path = hdfs_base_path
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        
        # Camera topics
        self.camera_topics = {
            f'cam{i}': f'cam{i}-feed' for i in range(1, 8)
        }
        
        logger.info(f"Initialized Wildtrack Producer for {len(self.camera_topics)} cameras")
        logger.info(f"Using {'HDFS' if use_hdfs else 'Local'} storage")
    
    def hdfs_command(self, cmd):
        """Execute HDFS command via docker"""
        full_cmd = f"docker exec namenode hdfs dfs {cmd}"
        result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout, result.stderr
    
    def download_from_hdfs(self, hdfs_path, local_path):
        """Download file from HDFS to local temporary location"""
        success, stdout, stderr = self.hdfs_command(f"-get {hdfs_path} {local_path}")
        if not success:
            logger.error(f"Failed to download from HDFS: {stderr}")
            return False
        return True
    
    def load_annotations(self, frame_number):
        """Load annotations for specific frame from HDFS or local"""
        if self.use_hdfs:
            # Download annotation file from HDFS
            hdfs_annotation_path = f"{self.hdfs_base_path}/annotations/annotations_positions/{frame_number:08d}.json"
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
                temp_path = temp_file.name
            
            success = self.download_from_hdfs(hdfs_annotation_path, temp_path)
            if success and os.path.exists(temp_path):
                try:
                    with open(temp_path, 'r') as f:
                        annotations = json.load(f)
                    os.unlink(temp_path)  # Clean up
                    return annotations
                except:
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
                    return []
            else:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                return []
        else:
            # Local filesystem fallback
            annotation_file = Path(f'../data/wildtrack/annotations_positions/{frame_number:08d}.json')
            if annotation_file.exists():
                with open(annotation_file, 'r') as f:
                    return json.load(f)
        return []
    
    def get_video_path(self, camera_id):
        """Get video path (HDFS or local)"""
        if self.use_hdfs:
            return f"{self.hdfs_base_path}/videos/cam{camera_id}.mp4"
        else:
            return f"../data/wildtrack/cam{camera_id}.mp4"
    
    def open_video_capture(self, camera_id):
        """Open video capture from HDFS or local filesystem"""
        if self.use_hdfs:
            # Download video from HDFS to temporary location
            hdfs_video_path = f"{self.hdfs_base_path}/videos/cam{camera_id}.mp4"
            
            with tempfile.NamedTemporaryFile(suffix=f'_cam{camera_id}.mp4', delete=False) as temp_file:
                temp_path = temp_file.name
            
            logger.info(f"Downloading cam{camera_id}.mp4 from HDFS...")
            success = self.download_from_hdfs(hdfs_video_path, temp_path)
            
            if success and os.path.exists(temp_path):
                cap = cv2.VideoCapture(temp_path)
                if cap.isOpened():
                    logger.info(f"Successfully opened cam{camera_id}.mp4 from HDFS")
                    return cap, temp_path
                else:
                    logger.error(f"Failed to open video: cam{camera_id}.mp4")
                    os.unlink(temp_path)
                    return None, None
            else:
                logger.error(f"Failed to download cam{camera_id}.mp4 from HDFS")
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                return None, None
        else:
            # Local filesystem
            video_path = f"../data/wildtrack/cam{camera_id}.mp4"
            if os.path.exists(video_path):
                cap = cv2.VideoCapture(video_path)
                return cap, None  # No temp file to clean up
            else:
                logger.error(f"Video file not found: {video_path}")
                return None, None
    
    def position_id_to_coordinates(self, position_id):
        """Convert positionID to 3D coordinates"""
        x = -3.0 + 0.025 * (position_id % 480)
        y = -9.0 + 0.025 * (position_id // 480)
        return x, y
    
    def encode_frame(self, frame):
        """Encode frame to base64 for Kafka transmission"""
        _, buffer = cv2.imencode('.jpg', frame)
        return base64.b64encode(buffer).decode('utf-8')
    
    def stream_camera(self, camera_id, fps=10, start_frame=0, max_frames=None):
        """
        Stream single camera video to Kafka from HDFS
        
        Args:
            camera_id: Camera ID (1-7)
            fps: Target FPS for streaming
            start_frame: Starting frame number
            max_frames: Maximum frames to stream (None for all)
        """
        topic = self.camera_topics[f'cam{camera_id}']
        
        cap, temp_file = self.open_video_capture(camera_id)
        if cap is None:
            return
        
        cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
        
        frame_interval = 1.0 / fps
        frame_count = start_frame
        
        logger.info(f"Starting camera {camera_id} stream to topic '{topic}' (from {'HDFS' if self.use_hdfs else 'local'})")
        
        try:
            while True:
                ret, frame = cap.read()
                if not ret:
                    logger.info(f"End of video for camera {camera_id}")
                    break
                
                if max_frames and (frame_count - start_frame) >= max_frames:
                    logger.info(f"Reached max frames ({max_frames}) for camera {camera_id}")
                    break
                
                # Load annotations for this frame
                annotations = self.load_annotations(frame_count)
                
                # Prepare message
                message = {
                    'camera_id': camera_id,
                    'frame_number': frame_count,
                    'timestamp': time.time(),
                    'frame_data': self.encode_frame(frame),
                    'annotations': [],
                    'source': 'hdfs' if self.use_hdfs else 'local'
                }
                
                # Add 3D position annotations
                for ann in annotations:
                    if 'positionID' in ann:
                        x, y = self.position_id_to_coordinates(ann['positionID'])
                        message['annotations'].append({
                            'position_id': ann['positionID'],
                            'x_coord': x,
                            'y_coord': y,
                            'person_id': ann.get('personID', -1)
                        })
                
                # Send to Kafka
                self.producer.send(topic, key=f'cam{camera_id}_{frame_count}', value=message)
                
                if frame_count % 100 == 0:
                    logger.info(f"Camera {camera_id}: Sent frame {frame_count} (annotations: {len(message['annotations'])})")
                
                frame_count += 1
                time.sleep(frame_interval)
                
        except KeyboardInterrupt:
            logger.info(f"Interrupted camera {camera_id} stream")
        finally:
            cap.release()
            if temp_file and os.path.exists(temp_file):
                os.unlink(temp_file)
                logger.info(f"Cleaned up temporary file for camera {camera_id}")
            logger.info(f"Released camera {camera_id}")
    
    def stream_all_cameras(self, fps=10, start_frame=0, max_frames=None):
        """
        Stream all 7 cameras simultaneously using threading
        """
        import threading
        
        threads = []
        for camera_id in range(1, 8):
            thread = threading.Thread(
                target=self.stream_camera,
                args=(camera_id, fps, start_frame, max_frames),
                daemon=True
            )
            threads.append(thread)
            thread.start()
            
            # Small delay between camera starts
            time.sleep(0.1)
        
        try:
            # Wait for all threads
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            logger.info("Stopping all camera streams...")
        
        self.producer.close()
        logger.info("All cameras stopped")

def main():
    parser = argparse.ArgumentParser(description='Wildtrack Kafka Producer with HDFS support')
    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--use-hdfs', action='store_true', default=True, help='Use HDFS for data storage')
    parser.add_argument('--hdfs-base-path', default='/surveillance/wildtrack', help='Base path in HDFS')
    parser.add_argument('--camera', type=int, choices=range(1, 8), help='Stream single camera (1-7)')
    parser.add_argument('--fps', type=int, default=10, help='Target FPS for streaming')
    parser.add_argument('--start-frame', type=int, default=0, help='Starting frame number')
    parser.add_argument('--max-frames', type=int, help='Maximum frames to stream')
    
    args = parser.parse_args()
    
    producer = WildtrackKafkaProducer(
        kafka_servers=args.kafka_servers,
        use_hdfs=args.use_hdfs,
        hdfs_base_path=args.hdfs_base_path
    )
    
    if args.camera:
        # Stream single camera
        producer.stream_camera(args.camera, args.fps, args.start_frame, args.max_frames)
    else:
        # Stream all cameras
        producer.stream_all_cameras(args.fps, args.start_frame, args.max_frames)

if __name__ == '__main__':
    main()
