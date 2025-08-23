# BDA Surveillance System

A distributed real-time video analytics system for multi-camera person tracking and re-identification.

## Architecture Overview

```
MARS Dataset → Kafka Producers → Kafka Cluster → Spark Streaming → Detection/Tracking/ReID → Results → Dashboard
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
- **Grafana Dashboard**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090

### 3. Database Connection

- **Host**: localhost:5432
- **Database**: surveillance_db
- **Username**: surveillance_user
- **Password**: surveillance_pass

## Project Structure

```
bda-surveillance/
├── kafka-producers/          # MARS dataset streaming simulators
├── spark-streaming/          # Scala processing jobs
├── models/                   # YOLO, OSNet model files
├── dashboard/               # Web interface (React/Flask)
├── scripts/                 # Setup and utility scripts
├── monitoring/              # Prometheus configuration
├── init-scripts/           # Database initialization
├── data/                   # MARS dataset location
├── docker-compose.yml      # Infrastructure definition
└── PROJECT.md             # Detailed methodology
```

## Development Workflow

### Phase 1: Data Ingestion ✅

- [x] Infrastructure setup
- [ ] MARS dataset integration
- [ ] Kafka producers for camera simulation

### Phase 2: Core Processing

- [ ] YOLO detection in Spark
- [ ] DeepSORT tracking implementation
- [ ] OSNet re-identification

### Phase 3: Analytics

- [ ] Global trajectory construction
- [ ] Person search functionality
- [ ] Real-time heatmaps

### Phase 4: Dashboard

- [ ] Live feed visualization
- [ ] Analytics interface
- [ ] Query system

## Next Steps

1. **Download MARS Dataset**:

   ```bash
   # Place MARS dataset in ./data/ directory
   mkdir -p data/MARS
   # Download from: http://zheng-lab.cecs.anu.edu.au/Project/project_mars.html
   ```

2. **Implement Kafka Producer**:

   ```bash
   cd kafka-producers
   # Create Python producer for MARS dataset streaming
   ```

3. **Develop Spark Streaming Job**:
   ```bash
   cd spark-streaming
   # Create Scala application for real-time processing
   ```

## Monitoring & Debugging

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
