# BDA Surveillance System

A distributed real-time video analytics system for multi-camera person tracking and re-identification using the Wildtrack dataset.

## Architecture Overview

```
Wildtrack Dataset → Kafka Producers → Kafka Cluster → Spark Streaming → Detection/Tracking/ReID → Results → Dashboard
                                         ↓
                                HDFS (Storage) ← PostgreSQL (Metadata) ← Elasticsearch (Embeddings)
```

## Infrastructure Components

### 🏗️ **Big Data Stack**

- **Hadoop Cluster**: HDFS for distributed storage
- **Kafka Cluster**: 2-broker setup for video stream ingestion
- **Spark Cluster**: Master + 2 Workers for distributed processing

### 🗄️ **Data Layer**

- **PostgreSQL**: Person embeddings, trajectories, detections
- **Redis**: Caching for real-time queries
- **Elasticsearch**: Fast embedding similarity search

### 📊 **Monitoring**

- **Prometheus**: Metrics collection
- **Grafana**: Visualization dashboards
- **Kafka UI**: Stream monitoring

## Quick Start

### Prerequisites

- Docker & Docker Compose
- At least 8GB RAM
- 20GB free disk space

> **Note**: If you have existing services running on ports 3000, 6379, or 5432, the setup will automatically use alternative ports (see [Port Configuration](#port-configuration) below).

### 1. Setup Infrastructure

```bash
# Clone and navigate to project
cd bda-surveillance

# Make scripts executable
chmod +x scripts/*.sh

# Start all services
./scripts/setup.sh
```

### 2. Access Services

- **Hadoop NameNode UI**: http://localhost:9870
- **Kafka UI**: http://localhost:8080
- **Spark Master UI**: http://localhost:8081
- **Grafana Dashboard**: http://localhost:3001 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Elasticsearch**: http://localhost:9200

### 3. Database Connections

**PostgreSQL:**
- **Host**: localhost:5433
- **Database**: surveillance_db
- **Username**: surveillance_user
- **Password**: surveillance_pass

**Redis Cache:**
- **Host**: localhost:6380
- **Default DB**: 0

## Port Configuration

The following ports have been configured to avoid common conflicts:

| Service | Default Port | Configured Port | Reason |
|---------|-------------|-----------------|---------|
| Redis | 6379 | **6380** | Avoid conflict with existing Redis instances |
| Grafana | 3000 | **3001** | Avoid conflict with development servers |
| PostgreSQL | 5432 | **5433** | Avoid conflict with local PostgreSQL |

If you need to change these ports further, edit the `docker-compose.yml` file and restart the services.

## Project Structure

```
bda-surveillance/
├── kafka-producers/          # Wildtrack dataset streaming simulators
├── spark-streaming/          # Scala processing jobs
├── models/                   # YOLO, OSNet model files
├── dashboard/               # Web interface (React/Flask)
├── scripts/                 # Setup and utility scripts
├── monitoring/              # Prometheus configuration
├── init-scripts/           # Database initialization
├── data/                   # Wildtrack dataset location
│   └── wildtrack/          # 7-camera videos + annotations
├── docker-compose.yml      # Infrastructure definition
└── PROJECT.md             # Detailed methodology
```

## Development Workflow

### Phase 1: Data Ingestion ✅

- [x] Infrastructure setup
- [x] HDFS-ready architecture
- [ ] Wildtrack dataset in HDFS (run `./scripts/download_wildtrack.sh`)
- [ ] HDFS-enabled Kafka producers

### Phase 2: Core Processing

- [ ] YOLO detection in Spark
- [ ] DeepSORT tracking implementation  
- [ ] OSNet re-identification across 7 cameras

### Phase 3: Analytics

- [ ] Global trajectory construction with 3D coordinates
- [ ] Person search functionality
- [ ] Real-time heatmaps using Wildtrack ground plane

### Phase 4: Dashboard

- [ ] Live feed visualization (7 camera views)
- [ ] 3D trajectory visualization
- [ ] Analytics interface with camera calibration support

## Next Steps

## Next Steps

1. **Download Wildtrack Dataset to HDFS**:

   ```bash
   # Download and upload dataset to HDFS
   ./scripts/download_wildtrack.sh
   
   # Verify HDFS storage
   # Access HDFS Web UI: http://localhost:9870
   # Navigate to: /surveillance/wildtrack/
   ```

2. **Stream from HDFS via Kafka**:

   ```bash
   cd kafka-producers
   # Stream all 7 cameras from HDFS
   python wildtrack_producer.py --use-hdfs --fps 10
   ```

3. **Develop Spark Streaming Job**:
   ```bash
   cd spark-streaming
   # Create Scala application for HDFS-based processing
   # Process 7 concurrent camera streams with cross-camera tracking
   ```

## Monitoring & Debugging

### Port Conflicts

If you encounter port conflicts during setup:

1. **Check which service is using the port**:
   ```bash
   sudo lsof -i :6379  # Check Redis port
   sudo lsof -i :3000  # Check Grafana port
   sudo lsof -i :5432  # Check PostgreSQL port
   ```

2. **Stop conflicting services** (if safe to do so):
   ```bash
   # Stop local Redis
   sudo systemctl stop redis
   
   # Stop local PostgreSQL
   sudo systemctl stop postgresql
   ```

3. **Or modify ports in docker-compose.yml** and restart:
   ```bash
   docker-compose down
   ./scripts/setup.sh
   ```

### View Logs

```bash
# View all service logs
docker-compose logs -f

# View specific service
docker-compose logs -f kafka1
docker-compose logs -f spark-master
```

### Resource Usage

```bash
# Check container resource usage
docker stats

# Check disk usage
docker system df
```

### Cleanup

```bash
# Stop all services
./scripts/shutdown.sh

# Remove volumes (optional)
docker volume prune -f
```

## Technology Stack

| Component  | Technology           | Purpose                     |
| ---------- | -------------------- | --------------------------- |
| Streaming  | Apache Kafka         | Video feed ingestion        |
| Processing | Apache Spark (Scala) | Distributed analytics       |
| Storage    | Hadoop HDFS          | Distributed file storage    |
| Database   | PostgreSQL           | Metadata & embeddings       |
| Cache      | Redis                | Real-time queries           |
| Search     | Elasticsearch        | Embedding similarity        |
| Detection  | YOLOv8/v9            | Person detection            |
| Tracking   | DeepSORT/ByteTrack   | Multi-object tracking       |
| Re-ID      | OSNet                | Cross-camera identification |
| Monitoring | Prometheus + Grafana | System monitoring           |

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## License

This project is part of academic research in Big Data Analytics.
