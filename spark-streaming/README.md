# Spark Streaming Jobs (HDFS-enabled)

This directory contains Spark Structured Streaming applications for processing Wildtrack dataset from HDFS.

## Structure

```
spark-streaming/
├── src/
│   └── main/
│       └── scala/
│           ├── WildtrackProcessor.scala      # Main streaming job
│           ├── PersonDetector.scala          # YOLO detection
│           ├── PersonTracker.scala           # DeepSORT tracking
│           └── PersonReIdentifier.scala      # OSNet re-identification
├── build.sbt                                # Scala build configuration
├── docker/
│   └── Dockerfile                           # Custom Spark image with dependencies
└── README.md
```

## Building

```bash
# Build Scala application
sbt package

# Build Docker image with dependencies
docker build -t surveillance-spark:latest docker/
```

## Deployment

```bash
# Submit to Spark cluster
docker exec spark-master spark-submit \
    --class com.surveillance.WildtrackProcessor \
    --master spark://spark-master:7077 \
    /opt/spark-apps/target/scala-2.12/wildtrack-processor_2.12-1.0.jar
```

## HDFS Integration

- **Input**: Reads video frames from Kafka topics
- **Models**: Loads YOLO/OSNet models from HDFS (`/surveillance/models/`)
- **Output**: Writes results to HDFS (`/surveillance/results/`)
- **Checkpoints**: Spark streaming checkpoints in HDFS

## Data Flow

```
Kafka Topics → Spark Streaming → HDFS Results
     ↓              ↓               ↓
  Frame Data → Detection/Tracking → Trajectories
                    ↓
              Person Embeddings → PostgreSQL
```
