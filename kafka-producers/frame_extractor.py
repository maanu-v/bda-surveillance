#!/usr/bin/env python3
"""
Frame extraction utilities for video processing.
"""

import cv2
import numpy as np
from pathlib import Path
import logging
from typing import Generator, Tuple, Optional

logger = logging.getLogger(__name__)

class FrameExtractor:
    """Extract frames from video files with various options."""
    
    def __init__(self, video_path: str):
        self.video_path = Path(video_path)
        self.cap = None
        
    def __enter__(self):
        self.cap = cv2.VideoCapture(str(self.video_path))
        if not self.cap.isOpened():
            raise ValueError(f"Cannot open video: {self.video_path}")
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cap:
            self.cap.release()
    
    def get_video_info(self) -> dict:
        """Get video metadata."""
        if not self.cap:
            return {}
            
        return {
            'total_frames': int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT)),
            'fps': self.cap.get(cv2.CAP_PROP_FPS),
            'width': int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
            'height': int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
            'duration': int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT)) / self.cap.get(cv2.CAP_PROP_FPS)
        }
    
    def extract_frames(self, 
                      start_frame: int = 0,
                      max_frames: Optional[int] = None,
                      step: int = 1) -> Generator[Tuple[int, np.ndarray], None, None]:
        """
        Extract frames from video.
        
        Args:
            start_frame: Starting frame number
            max_frames: Maximum number of frames to extract
            step: Frame step (1 = every frame, 2 = every other frame, etc.)
            
        Yields:
            Tuple of (frame_number, frame_array)
        """
        if not self.cap:
            return
            
        # Set starting position
        self.cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
        
        frame_count = 0
        current_frame = start_frame
        
        while True:
            ret, frame = self.cap.read()
            
            if not ret:
                break
                
            if max_frames and frame_count >= max_frames:
                break
                
            # Skip frames based on step
            if (current_frame - start_frame) % step == 0:
                yield current_frame, frame
                frame_count += 1
                
            current_frame += 1
    
    def extract_frame_at(self, frame_number: int) -> Optional[np.ndarray]:
        """Extract a specific frame."""
        if not self.cap:
            return None
            
        self.cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
        ret, frame = self.cap.read()
        
        return frame if ret else None
    
    def resize_frame(self, frame: np.ndarray, 
                    target_width: int = None, 
                    target_height: int = None,
                    maintain_aspect: bool = True) -> np.ndarray:
        """Resize frame with optional aspect ratio preservation."""
        
        if not target_width and not target_height:
            return frame
            
        height, width = frame.shape[:2]
        
        if maintain_aspect:
            if target_width and target_height:
                # Calculate aspect ratios
                aspect_original = width / height
                aspect_target = target_width / target_height
                
                if aspect_original > aspect_target:
                    # Original is wider
                    new_width = target_width
                    new_height = int(target_width / aspect_original)
                else:
                    # Original is taller
                    new_height = target_height
                    new_width = int(target_height * aspect_original)
            elif target_width:
                new_width = target_width
                new_height = int(target_width * height / width)
            else:
                new_height = target_height
                new_width = int(target_height * width / height)
        else:
            new_width = target_width or width
            new_height = target_height or height
            
        return cv2.resize(frame, (new_width, new_height))

def test_frame_extraction():
    """Test frame extraction functionality."""
    
    # Test with a sample video
    video_path = "../data/wildtrack/cam1.mp4"
    
    if not Path(video_path).exists():
        print(f"Test video not found: {video_path}")
        return
        
    print("Testing frame extraction...")
    
    with FrameExtractor(video_path) as extractor:
        # Get video info
        info = extractor.get_video_info()
        print(f"Video info: {info}")
        
        # Extract first 5 frames
        print("Extracting first 5 frames...")
        for frame_num, frame in extractor.extract_frames(max_frames=5):
            print(f"Frame {frame_num}: {frame.shape}")
            
        # Extract specific frame
        frame_100 = extractor.extract_frame_at(100)
        if frame_100 is not None:
            print(f"Frame 100: {frame_100.shape}")
        
        print("Frame extraction test completed!")

if __name__ == "__main__":
    test_frame_extraction()
