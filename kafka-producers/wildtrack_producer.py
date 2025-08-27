#!/usr/bin/env python3
"""
Wildtrack Dataset Kafka Producer
Streams video frames from 7 cameras to Kafka topics for real-time processing.
"""

import cv2
import json
import time
import argparse
import logging
from datetime import datetime
from pathlib import Path
import numpy as np
from kafka import KafkaProducer
from kafka.errors import KafkaError
import base64
import os
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WildtrackProducer:
    """Kafka producer for Wildtrack 7-camera dataset streaming."""
    
    def __init__(self, 
                 bootstrap_servers: List[str] = ['localhost:9092', 'localhost:9093'],
                 data_path: str = '../data/wildtrack',
                 fps: int = 10,
                 use_hdfs: bool = False):
        """
        Initialize Wildtrack Kafka Producer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            data_path: Path to Wildtrack dataset 
            fps: Frames per second for streaming
            use_hdfs: Whether to read from HDFS (future implementation)
        """
        self.bootstrap_servers = bootstrap_servers
        self.data_path = Path(data_path)
        self.fps = fps
        self.frame_interval = 1.0 / fps
        self.use_hdfs = use_hdfs
        
        # Camera configuration
        self.cameras = [f'cam{i}' for i in range(1, 8)]  # cam1 to cam7
        self.camera_topics = {cam: f'{cam}-feed' for cam in self.cameras}
        
        # Initialize Kafka producer
        self.producer = self._create_producer()
        
        # Load annotations and calibrations
        self.annotations = self._load_annotations()
        self.calibrations = self._load_calibrations()
        
        logger.info(f"Initialized WildtrackProducer with {len(self.cameras)} cameras")
        logger.info(f"Streaming at {fps} FPS to topics: {list(self.camera_topics.values())}")

    def _create_producer(self) -> KafkaProducer:
        """Create and configure Kafka producer."""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip',
                buffer_memory=33554432,  # 32MB buffer
                batch_size=16384,
                linger_ms=10
            )
            logger.info("Kafka producer created successfully")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise

    def _load_annotations(self) -> Dict:
        """Load frame annotations from JSON files."""
        annotations = {}
        annotations_dir = self.data_path / 'annotations_positions'
        
        if not annotations_dir.exists():
            logger.warning(f"Annotations directory not found: {annotations_dir}")
            return annotations
            
        try:
            for json_file in annotations_dir.glob('*.json'):
                frame_id = json_file.stem  # e.g., '00000000'
                with open(json_file, 'r') as f:
                    annotations[frame_id] = json.load(f)
            logger.info(f"Loaded {len(annotations)} annotation files")
        except Exception as e:
            logger.error(f"Error loading annotations: {e}")
            
        return annotations

    def _load_calibrations(self) -> Dict:
        """Load camera calibration data."""
        calibrations = {}
        calibrations_dir = self.data_path / 'calibrations'
        
        if not calibrations_dir.exists():
            logger.warning(f"Calibrations directory not found: {calibrations_dir}")
            return calibrations
            
        # This would load intrinsic/extrinsic camera parameters
        # For now, return empty dict - will be implemented when needed
        logger.info("Camera calibrations loading placeholder")
        return calibrations

    def _convert_position_to_3d(self, position_id: int) -> Dict[str, float]:
        """
        Convert Wildtrack position ID to 3D coordinates.
        
        Grid: 480x1440 with 2.5cm spacing, origin at (-3.0, -9.0)
        """
        if position_id < 0:
            return {"x": 0.0, "y": 0.0, "z": 0.0}
            
        x = -3.0 + 0.025 * (position_id % 480)
        y = -9.0 + 0.025 * (position_id // 480)
        z = 0.0  # Assuming ground level
        
        return {"x": x, "y": y, "z": z}

    def _encode_frame(self, frame: np.ndarray) -> str:
        """Encode frame as base64 string for Kafka transport."""
        try:
            # Compress frame to JPEG
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 85]
            _, buffer = cv2.imencode('.jpg', frame, encode_param)
            
            # Convert to base64
            frame_base64 = base64.b64encode(buffer).decode('utf-8')
            return frame_base64
        except Exception as e:
            logger.error(f"Frame encoding failed: {e}")
            return ""

    def _create_frame_message(self, 
                             camera_id: str, 
                             frame: np.ndarray, 
                             frame_number: int,
                             timestamp: datetime) -> Dict:
        """Create structured message for Kafka."""
        
        # Get frame annotations if available
        frame_id = f"{frame_number:08d}"  # Zero-padded frame ID
        frame_annotations = self.annotations.get(frame_id, [])
        
        # Convert annotations to include 3D positions
        persons = []
        for annotation in frame_annotations:
            if isinstance(annotation, dict) and 'positionID' in annotation:
                position_3d = self._convert_position_to_3d(annotation['positionID'])
                persons.append({
                    'position_id': annotation['positionID'],
                    'position_3d': position_3d,
                    'person_id': annotation.get('personID', -1)
                })
        
        # Encode frame
        encoded_frame = self._encode_frame(frame)
        
        message = {
            'camera_id': camera_id,
            'frame_number': frame_number,
            'timestamp': timestamp.isoformat(),
            'frame_data': encoded_frame,
            'frame_shape': frame.shape,
            'persons': persons,
            'metadata': {
                'fps': self.fps,
                'dataset': 'wildtrack',
                'total_persons': len(persons)
            }
        }
        
        return message

    def _get_video_path(self, camera_id: str) -> Path:
        """Get video file path for camera."""
        if self.use_hdfs:
            # TODO: Implement HDFS reading
            raise NotImplementedError("HDFS reading not yet implemented")
        else:
            return self.data_path / f'{camera_id}.mp4'

    def stream_camera(self, camera_id: str, max_frames: Optional[int] = None):
        """Stream frames from a single camera to Kafka."""
        
        video_path = self._get_video_path(camera_id)
        
        if not video_path.exists():
            logger.error(f"Video file not found: {video_path}")
            return
            
        topic = self.camera_topics[camera_id]
        
        try:
            cap = cv2.VideoCapture(str(video_path))
            
            if not cap.isOpened():
                logger.error(f"Failed to open video: {video_path}")
                return
                
            # Get video properties
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            original_fps = cap.get(cv2.CAP_PROP_FPS)
            
            logger.info(f"Streaming {camera_id}: {total_frames} frames at {self.fps} FPS")
            logger.info(f"Original video FPS: {original_fps}, Target FPS: {self.fps}")
            
            frame_count = 0
            start_time = time.time()
            
            while True:
                ret, frame = cap.read()
                
                if not ret:
                    logger.info(f"End of video reached for {camera_id}")
                    break
                    
                if max_frames and frame_count >= max_frames:
                    logger.info(f"Max frames ({max_frames}) reached for {camera_id}")
                    break
                
                # Create message
                timestamp = datetime.now()
                message = self._create_frame_message(camera_id, frame, frame_count, timestamp)
                
                # Send to Kafka
                try:
                    future = self.producer.send(topic, key=camera_id, value=message)
                    # Don't wait for each message to avoid blocking
                    
                    frame_count += 1
                    
                    # Log progress
                    if frame_count % 100 == 0:
                        logger.info(f"{camera_id}: Sent {frame_count}/{total_frames} frames")
                        
                except KafkaError as e:
                    logger.error(f"Kafka send error for {camera_id}: {e}")
                    
                # Control frame rate
                elapsed = time.time() - start_time
                expected_time = frame_count * self.frame_interval
                if elapsed < expected_time:
                    time.sleep(expected_time - elapsed)
                    
            cap.release()
            logger.info(f"Finished streaming {camera_id}: {frame_count} frames sent")
            
        except Exception as e:
            logger.error(f"Error streaming {camera_id}: {e}")

    def stream_all_cameras(self, max_frames_per_camera: Optional[int] = None):
        """Stream all 7 cameras concurrently."""
        import threading
        
        logger.info("Starting concurrent streaming for all 7 cameras")
        
        threads = []
        for camera_id in self.cameras:
            thread = threading.Thread(
                target=self.stream_camera,
                args=(camera_id, max_frames_per_camera),
                name=f"Stream-{camera_id}"
            )
            threads.append(thread)
            thread.start()
            
            # Small delay between camera starts
            time.sleep(0.5)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
            
        logger.info("All camera streams completed")

    def create_topics(self):
        """Create Kafka topics for all cameras."""
        from kafka.admin import KafkaAdminClient, NewTopic
        
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='wildtrack-admin'
            )
            
            # Create topics
            topics = []
            for topic_name in self.camera_topics.values():
                topic = NewTopic(
                    name=topic_name,
                    num_partitions=3,
                    replication_factor=2
                )
                topics.append(topic)
            
            # Create topics
            fs = admin_client.create_topics(new_topics=topics, validate_only=False)
            
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    logger.info(f"Created topic: {topic}")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.info(f"Topic already exists: {topic}")
                    else:
                        logger.error(f"Failed to create topic {topic}: {e}")
                        
        except Exception as e:
            logger.error(f"Error creating topics: {e}")

    def close(self):
        """Clean up resources."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Wildtrack Kafka Producer')
    parser.add_argument('--fps', type=int, default=10, help='Frames per second (default: 10)')
    parser.add_argument('--max-frames', type=int, help='Max frames per camera (for testing)')
    parser.add_argument('--camera', type=str, help='Stream single camera (cam1-cam7)')
    parser.add_argument('--data-path', type=str, default='../data/wildtrack', 
                       help='Path to Wildtrack dataset')
    parser.add_argument('--use-hdfs', action='store_true', help='Read from HDFS (not implemented)')
    parser.add_argument('--create-topics', action='store_true', help='Create Kafka topics')
    
    args = parser.parse_args()
    
    # Initialize producer
    producer = WildtrackProducer(
        data_path=args.data_path,
        fps=args.fps,
        use_hdfs=args.use_hdfs
    )
    
    try:
        # Create topics if requested
        if args.create_topics:
            logger.info("Creating Kafka topics...")
            producer.create_topics()
            
        # Stream videos
        if args.camera:
            if args.camera in producer.cameras:
                logger.info(f"Streaming single camera: {args.camera}")
                producer.stream_camera(args.camera, args.max_frames)
            else:
                logger.error(f"Invalid camera: {args.camera}. Valid: {producer.cameras}")
                return
        else:
            logger.info("Streaming all 7 cameras")
            producer.stream_all_cameras(args.max_frames)
            
    except KeyboardInterrupt:
        logger.info("Streaming interrupted by user")
    except Exception as e:
        logger.error(f"Streaming failed: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
