# Kafka Producers for Wildtrack Dataset (HDFS-enabled)

This directory contains Kafka producers for streaming the Wildtrack dataset from **Hadoop HDFS** to simulate real-time multi-camera feeds.

## Files

- `wildtrack_producer.py`: Main producer for streaming 7-camera Wildtrack videos from HDFS
- `requirements.txt`: Python dependencies

## Prerequisites

1. **Infrastructure running**: `./scripts/setup.sh`
2. **Dataset in HDFS**: `./scripts/download_wildtrack.sh`

## Installation

```bash
# Install dependencies
pip install -r requirements.txt
```

## Usage

### Stream All 7 Cameras from HDFS

```bash
python wildtrack_producer.py --use-hdfs --fps 10
```

### Stream Single Camera from HDFS

```bash
python wildtrack_producer.py --camera 1 --use-hdfs --fps 10
```

### Stream from Local Filesystem (fallback)

```bash
python wildtrack_producer.py --no-use-hdfs --fps 10
```

### Advanced HDFS Options

```bash
# Stream with custom HDFS path and parameters
python wildtrack_producer.py \
    --use-hdfs \
    --hdfs-base-path /surveillance/wildtrack \
    --kafka-servers localhost:9092 \
    --fps 15 \
    --start-frame 1000 \
    --max-frames 5000
```

## Kafka Topics

The producer creates the following topics:

- `cam1-feed` - Camera 1 stream
- `cam2-feed` - Camera 2 stream  
- `cam3-feed` - Camera 3 stream
- `cam4-feed` - Camera 4 stream
- `cam5-feed` - Camera 5 stream
- `cam6-feed` - Camera 6 stream
- `cam7-feed` - Camera 7 stream

## Message Format (HDFS-enhanced)

Each Kafka message contains:

```json
{
  "camera_id": 1,
  "frame_number": 1000,
  "timestamp": 1692876543.123,
  "frame_data": "base64_encoded_image",
  "source": "hdfs",
  "annotations": [
    {
      "position_id": 12345,
      "x_coord": 2.5,
      "y_coord": 1.2,
      "person_id": 42
    }
  ]
}
```

## HDFS Integration

- **Videos**: Stored in `/surveillance/wildtrack/videos/` (cam1.mp4 to cam7.mp4)
- **Annotations**: Stored in `/surveillance/wildtrack/annotations/annotations_positions/`
- **Auto-download**: Producer automatically downloads required files from HDFS
- **Temp cleanup**: Temporary files are automatically cleaned up

## Monitoring

- Use Kafka UI at http://localhost:8080 to monitor topics
- Check producer logs for streaming status
- Monitor frame rates and message counts

## Performance Tips

- **HDFS advantage**: Better for large datasets and distributed processing
- Adjust `--fps` based on your processing capacity
- Use `--max-frames` for testing with limited data
- Monitor HDFS usage: http://localhost:9870
- Monitor Kafka broker memory usage with high throughput
- **Pre-download**: Large videos are temporarily downloaded from HDFS per camera
