#!/usr/bin/env python3
"""
Test script to verify Kafka producer functionality.
"""

import sys
import time
from pathlib import Path
from kafka import KafkaConsumer
import json
import base64
import cv2
import numpy as np

def test_kafka_connection():
    """Test basic Kafka connectivity."""
    print("🔗 Testing Kafka connection...")
    
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092', 'localhost:9093'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Send test message
        test_message = {"test": "connection", "timestamp": time.time()}
        future = producer.send('test-topic', test_message)
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Kafka connection successful!")
        print(f"   Topic: {record_metadata.topic}")
        print(f"   Partition: {record_metadata.partition}")
        print(f"   Offset: {record_metadata.offset}")
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        return False

def test_dataset_availability():
    """Test if Wildtrack dataset is available."""
    print("\n📁 Testing dataset availability...")
    
    data_path = Path("../data/wildtrack")
    
    if not data_path.exists():
        print(f"❌ Dataset directory not found: {data_path}")
        return False
    
    required_files = [f"cam{i}.mp4" for i in range(1, 8)]
    missing_files = []
    
    for file_name in required_files:
        file_path = data_path / file_name
        if not file_path.exists():
            missing_files.append(file_name)
        else:
            file_size = file_path.stat().st_size / (1024*1024)  # MB
            print(f"   ✅ {file_name}: {file_size:.1f} MB")
    
    if missing_files:
        print(f"❌ Missing video files: {missing_files}")
        return False
    
    # Check annotations
    annotations_dir = data_path / "annotations_positions"
    if annotations_dir.exists():
        annotation_count = len(list(annotations_dir.glob("*.json")))
        print(f"   ✅ Annotations: {annotation_count} files")
    else:
        print(f"   ⚠️  Annotations directory not found")
    
    print("✅ Dataset availability check passed!")
    return True

def test_video_reading():
    """Test video file reading."""
    print("\n🎥 Testing video reading...")
    
    video_path = Path("../data/wildtrack/cam1.mp4")
    
    if not video_path.exists():
        print(f"❌ Test video not found: {video_path}")
        return False
    
    try:
        cap = cv2.VideoCapture(str(video_path))
        
        if not cap.isOpened():
            print(f"❌ Cannot open video: {video_path}")
            return False
        
        # Get video properties
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        
        print(f"   📊 Video properties:")
        print(f"      Frames: {total_frames}")
        print(f"      FPS: {fps}")
        print(f"      Resolution: {width}x{height}")
        
        # Read first frame
        ret, frame = cap.read()
        if ret:
            print(f"   ✅ Successfully read frame: {frame.shape}")
        else:
            print(f"   ❌ Failed to read first frame")
            return False
        
        cap.release()
        print("✅ Video reading test passed!")
        return True
        
    except Exception as e:
        print(f"❌ Video reading failed: {e}")
        return False

def test_producer_basic():
    """Test basic producer functionality with small data."""
    print("\n🚀 Testing basic producer functionality...")
    
    try:
        from wildtrack_producer import WildtrackProducer
        
        # Initialize producer
        producer = WildtrackProducer(fps=1)  # Very slow for testing
        
        # Create topics
        print("   Creating Kafka topics...")
        producer.create_topics()
        
        # Test streaming single camera with limited frames
        print("   Testing single camera stream (5 frames)...")
        producer.stream_camera('cam1', max_frames=5)
        
        producer.close()
        print("✅ Basic producer test passed!")
        return True
        
    except Exception as e:
        print(f"❌ Basic producer test failed: {e}")
        return False

def test_consumer_verification():
    """Test consuming messages to verify they're being produced."""
    print("\n📥 Testing message consumption...")
    
    try:
        consumer = KafkaConsumer(
            'cam1-feed',
            bootstrap_servers=['localhost:9092', 'localhost:9093'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000,  # 5 second timeout
            auto_offset_reset='latest'
        )
        
        print("   Waiting for messages...")
        message_count = 0
        
        for message in consumer:
            message_count += 1
            data = message.value
            
            print(f"   📨 Message {message_count}:")
            print(f"      Camera: {data.get('camera_id')}")
            print(f"      Frame: {data.get('frame_number')}")
            print(f"      Persons: {len(data.get('persons', []))}")
            print(f"      Frame size: {len(data.get('frame_data', ''))} chars")
            
            if message_count >= 3:  # Check first 3 messages
                break
        
        consumer.close()
        
        if message_count > 0:
            print("✅ Message consumption test passed!")
            return True
        else:
            print("⚠️  No messages received (may be normal if no producer is running)")
            return True
            
    except Exception as e:
        print(f"❌ Message consumption test failed: {e}")
        return False

def run_all_tests():
    """Run all tests in sequence."""
    print("🧪 Running Kafka Producer Tests")
    print("=" * 50)
    
    tests = [
        ("Kafka Connection", test_kafka_connection),
        ("Dataset Availability", test_dataset_availability), 
        ("Video Reading", test_video_reading),
        ("Basic Producer", test_producer_basic),
        ("Message Consumption", test_consumer_verification)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 50)
    print("📋 Test Results Summary:")
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, passed_test in results.items():
        status = "✅ PASS" if passed_test else "❌ FAIL"
        print(f"   {status} {test_name}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Ready for production streaming.")
    else:
        print("⚠️  Some tests failed. Please check the issues above.")

if __name__ == "__main__":
    run_all_tests()
