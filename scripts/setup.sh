#!/bin/bash

# BDA Surveillance System Setup Script
echo "🚀 Setting up BDA Surveillance System..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print status
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker &> /dev/null; then
    print_error "Docker is not available. Please install Docker."
    exit 1
fi

print_status "Starting BDA Surveillance Infrastructure..."

# Start the infrastructure
docker compose up -d

if [ $? -eq 0 ]; then
    print_status "Infrastructure started successfully!"
else
    print_error "Failed to start infrastructure"
    exit 1
fi

# Wait for services to be ready
print_status "Waiting for services to be ready..."
sleep 30

# Check service health
print_status "Checking service health..."

services=(
    "namenode:9870"
    "kafka-ui:8080"
    "spark-master:8081"
    "postgres-db:5433"
    "elasticsearch:9200"
    "prometheus:9090"
    "grafana:3000"
)

for service in "${services[@]}"; do
    container_name=$(echo $service | cut -d':' -f1)
    port=$(echo $service | cut -d':' -f2)
    
    if docker ps | grep -q $container_name; then
        print_status "✅ $container_name is running"
    else
        print_warning "⚠️  $container_name is not running"
    fi
done

print_status "Setup complete! 🎉"
echo
echo "📊 Access your services:"
echo "  • Hadoop NameNode UI: http://localhost:9870"
echo "  • Kafka UI: http://localhost:8080"
echo "  • Spark Master UI: http://localhost:8081"
echo "  • Grafana Dashboard: http://localhost:3000 (admin/admin)"
echo "  • Prometheus: http://localhost:9090"
echo "  • Elasticsearch: http://localhost:9200"
echo
echo "🗄️  Database Connection:"
echo "  • Host: localhost:5433"
echo "  • Database: surveillance_db"
echo "  • Username: surveillance_user"
echo "  • Password: surveillance_pass"
echo
print_status "Next steps:"
echo "  1. Download Wildtrack dataset to ./data/wildtrack/ directory"
echo "  2. Run the Kafka producer to simulate 7-camera feeds"
echo "  3. Deploy Spark streaming jobs for multi-camera processing"
echo "  4. Access the dashboard for real-time monitoring and 3D analytics"
