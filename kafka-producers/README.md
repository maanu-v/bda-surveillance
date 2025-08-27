# Wildtrack Kafka Producers

This directory contains the Kafka producers that stream the Wildtrack 7-camera dataset to Kafka topics for real-time processing.

## 📁 Files Overview

- `wildtrack_producer.py` - Main producer script for streaming all 7 cameras
- `frame_extractor.py` - Utility for video frame extraction and processing  
- `test_producer.py` - Test script to verify producer functionality
- `config.yaml` - Configuration file for producer settings
- `requirements.txt` - Python dependencies

## 🚀 Quick Start

### 1. Install Dependencies

```bash
# Install Python dependencies
pip install -r requirements.txt
```

### 2. Verify Infrastructure

Make sure your BDA infrastructure is running:

```bash
# From project root
./scripts/setup.sh

# Verify services are running
docker ps
```

### 3. Test Setup

```bash
# Run tests to verify everything works
python test_producer.py
```

### 4. Create Kafka Topics

```bash
# Create topics for all 7 cameras
python wildtrack_producer.py --create-topics
```

### 5. Start Streaming

```bash
# Stream all cameras at 10 FPS
python wildtrack_producer.py --fps 10

# Stream single camera for testing
python wildtrack_producer.py --camera cam1 --max-frames 100

# Stream at different FPS
python wildtrack_producer.py --fps 5
```

## 📊 Usage Examples

### Basic Streaming
```bash
# Stream all 7 cameras at default 10 FPS
python wildtrack_producer.py

# Stream with limited frames for testing
python wildtrack_producer.py --max-frames 500
```

### Single Camera Testing
```bash
# Test single camera
python wildtrack_producer.py --camera cam1 --max-frames 50

# Different camera
python wildtrack_producer.py --camera cam3 --fps 5
```

### Performance Tuning
```bash
# High FPS streaming (requires good hardware)
python wildtrack_producer.py --fps 20

# Low FPS for testing
python wildtrack_producer.py --fps 1 --max-frames 10
```

## 📋 Command Line Options

```
--fps INT              Frames per second (default: 10)
--max-frames INT       Maximum frames per camera (for testing)
--camera STRING        Stream single camera (cam1-cam7)
--data-path STRING     Path to Wildtrack dataset (default: ../data/wildtrack)
--use-hdfs             Read from HDFS (not yet implemented)
--create-topics        Create Kafka topics before streaming
```

## 🎯 Expected Output

When streaming, you should see:

```
2025-08-27 10:30:00 - INFO - Initialized WildtrackProducer with 7 cameras
2025-08-27 10:30:00 - INFO - Streaming at 10 FPS to topics: ['cam1-feed', 'cam2-feed', ...]
2025-08-27 10:30:01 - INFO - Starting concurrent streaming for all 7 cameras
2025-08-27 10:30:01 - INFO - Streaming cam1: 2000 frames at 10 FPS
2025-08-27 10:30:01 - INFO - cam1: Sent 100/2000 frames
```

## 🔍 Kafka Topics Created

The producer creates these topics:

- `cam1-feed` - Camera 1 video stream
- `cam2-feed` - Camera 2 video stream  
- `cam3-feed` - Camera 3 video stream
- `cam4-feed` - Camera 4 video stream
- `cam5-feed` - Camera 5 video stream
- `cam6-feed` - Camera 6 video stream
- `cam7-feed` - Camera 7 video stream

## 📦 Message Format

Each Kafka message contains:

```json
{
  "camera_id": "cam1",
  "frame_number": 42,
  "timestamp": "2025-08-27T10:30:00.123456",
  "frame_data": "base64_encoded_jpeg_frame",
  "frame_shape": [480, 640, 3],
  "persons": [
    {
      "position_id": 12345,
      "position_3d": {"x": 1.25, "y": 2.50, "z": 0.0},
      "person_id": 1
    }
  ],
  "metadata": {
    "fps": 10,
    "dataset": "wildtrack", 
    "total_persons": 3
  }
}
```

## 🐛 Troubleshooting

### Kafka Connection Issues
```bash
# Check if Kafka is running
docker ps | grep kafka

# Check Kafka UI
http://localhost:8080
```

### Dataset Issues
```bash
# Verify dataset exists
ls -la ../data/wildtrack/

# Should see cam1.mp4 through cam7.mp4
```

### Performance Issues
```bash
# Reduce FPS for testing
python wildtrack_producer.py --fps 1

# Stream single camera
python wildtrack_producer.py --camera cam1 --max-frames 10
```

### Memory Issues
```bash
# Check Docker memory usage
docker stats

# Reduce frame quality in config.yaml
# quality: 85 -> 70
```

## 🔧 Configuration

Edit `config.yaml` to customize:

- **FPS**: Streaming frame rate
- **Quality**: JPEG compression quality (85 = high, 50 = medium)
- **Buffer settings**: Kafka performance tuning
- **Grid settings**: 3D coordinate conversion

## ⚡ Performance Tips

1. **Start with low FPS** (1-5) for initial testing
2. **Use single camera** testing before full 7-camera streaming
3. **Monitor Docker stats** for resource usage
4. **Reduce JPEG quality** if bandwidth is limited
5. **Use `--max-frames`** for bounded testing

## 🎯 Next Steps

After getting producers working:

1. **Verify messages in Kafka UI**: http://localhost:8080
2. **Implement Spark consumers** to process the streams
3. **Add YOLO detection** to the processing pipeline
4. **Scale up FPS** once processing is optimized

## 📚 Related Documentation

- [PROJECT.md](../PROJECT.md) - Overall system architecture
- [README.md](../README.md) - Infrastructure setup
- [Wildtrack Dataset](../data/README.md) - Dataset information
