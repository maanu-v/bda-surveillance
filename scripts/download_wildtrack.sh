#!/bin/bash

# Wildtrack Dataset Download and HDFS Setup Script
echo "📥 Downloading Wildtrack Dataset and Setting up HDFS Storage..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Check if infrastructure is running
print_step "Checking if infrastructure is running..."
if ! docker ps | grep -q namenode; then
    print_error "Hadoop infrastructure not running. Please run ./setup.sh first"
    exit 1
fi

# Create local temporary directory
TEMP_DIR="./temp_wildtrack"
HDFS_BASE_PATH="/surveillance/wildtrack"

print_step "Creating temporary download directory..."
mkdir -p $TEMP_DIR

# Download Wildtrack dataset (you'll need to modify the URL)
print_step "Downloading Wildtrack dataset..."
print_warning "Please ensure you have downloaded the Wildtrack dataset from Kaggle"
print_warning "Expected size: ~10GB"

# Check if dataset exists locally
if [ ! -d "./data/wildtrack" ] || [ -z "$(ls -A ./data/wildtrack)" ]; then
    print_warning "Dataset not found in ./data/wildtrack/"
    echo "Please download from: https://www.kaggle.com/datasets/your-wildtrack-dataset-link"
    echo "Extract to: ./data/wildtrack/"
    echo
    read -p "Have you downloaded and extracted the dataset to ./data/wildtrack/? (y/n): " confirm
    if [[ $confirm != [yY] ]]; then
        print_error "Please download the dataset first"
        exit 1
    fi
fi

# Verify dataset structure
print_step "Verifying dataset structure..."
required_files=(
    "data/wildtrack/cam1.mp4"
    "data/wildtrack/cam2.mp4" 
    "data/wildtrack/cam3.mp4"
    "data/wildtrack/cam4.mp4"
    "data/wildtrack/cam5.mp4"
    "data/wildtrack/cam6.mp4"
    "data/wildtrack/cam7.mp4"
    "data/wildtrack/Image_subsets"
    "data/wildtrack/annotations_positions"
    "data/wildtrack/calibrations"
)

for file in "${required_files[@]}"; do
    if [ ! -e "$file" ]; then
        print_error "Missing required file/directory: $file"
        exit 1
    fi
done

print_status "✅ Dataset structure verified"

# Wait for HDFS to be ready
print_step "Waiting for HDFS to be ready..."
sleep 10

# Create HDFS directories
print_step "Creating HDFS directory structure..."
docker exec namenode hdfs dfs -mkdir -p $HDFS_BASE_PATH
docker exec namenode hdfs dfs -mkdir -p $HDFS_BASE_PATH/videos
docker exec namenode hdfs dfs -mkdir -p $HDFS_BASE_PATH/images
docker exec namenode hdfs dfs -mkdir -p $HDFS_BASE_PATH/annotations
docker exec namenode hdfs dfs -mkdir -p $HDFS_BASE_PATH/calibrations

# Upload videos to HDFS
print_step "Uploading camera videos to HDFS..."
for i in {1..7}; do
    if [ -f "data/wildtrack/cam${i}.mp4" ]; then
        print_status "Uploading cam${i}.mp4..."
        docker exec namenode hdfs dfs -put /tmp/wildtrack/cam${i}.mp4 $HDFS_BASE_PATH/videos/ 2>/dev/null || \
        docker cp data/wildtrack/cam${i}.mp4 namenode:/tmp/ && \
        docker exec namenode hdfs dfs -put /tmp/cam${i}.mp4 $HDFS_BASE_PATH/videos/
    fi
done

# Upload Image_subsets to HDFS
print_step "Uploading image subsets to HDFS..."
if [ -d "data/wildtrack/Image_subsets" ]; then
    docker cp data/wildtrack/Image_subsets namenode:/tmp/
    docker exec namenode hdfs dfs -put /tmp/Image_subsets $HDFS_BASE_PATH/images/
fi

# Upload annotations to HDFS
print_step "Uploading annotations to HDFS..."
if [ -d "data/wildtrack/annotations_positions" ]; then
    docker cp data/wildtrack/annotations_positions namenode:/tmp/
    docker exec namenode hdfs dfs -put /tmp/annotations_positions $HDFS_BASE_PATH/annotations/
fi

# Upload calibrations to HDFS
print_step "Uploading calibrations to HDFS..."
if [ -d "data/wildtrack/calibrations" ]; then
    docker cp data/wildtrack/calibrations namenode:/tmp/
    docker exec namenode hdfs dfs -put /tmp/calibrations $HDFS_BASE_PATH/calibrations/
fi

# Upload other files
print_step "Uploading additional files..."
if [ -f "data/wildtrack/rectangles.pom" ]; then
    docker cp data/wildtrack/rectangles.pom namenode:/tmp/
    docker exec namenode hdfs dfs -put /tmp/rectangles.pom $HDFS_BASE_PATH/
fi

# Verify HDFS upload
print_step "Verifying HDFS upload..."
docker exec namenode hdfs dfs -ls -R $HDFS_BASE_PATH

# Get HDFS usage
print_step "Checking HDFS usage..."
docker exec namenode hdfs dfs -du -h $HDFS_BASE_PATH

# Cleanup temporary files in container
print_step "Cleaning up temporary files..."
docker exec namenode rm -rf /tmp/Image_subsets /tmp/annotations_positions /tmp/calibrations /tmp/cam*.mp4 /tmp/rectangles.pom

print_status "✅ Wildtrack dataset successfully uploaded to HDFS!"
echo
echo "📊 HDFS Structure:"
echo "  • Videos: $HDFS_BASE_PATH/videos/ (cam1.mp4 to cam7.mp4)"
echo "  • Images: $HDFS_BASE_PATH/images/Image_subsets/"
echo "  • Annotations: $HDFS_BASE_PATH/annotations/annotations_positions/"
echo "  • Calibrations: $HDFS_BASE_PATH/calibrations/"
echo
echo "🔗 Access HDFS Web UI: http://localhost:9870"
echo "   Navigate to: Utilities → Browse the file system → /surveillance/wildtrack"
echo
print_status "Next steps:"
echo "  1. Update Kafka producers to read from HDFS"
echo "  2. Configure Spark streaming to process HDFS data"
echo "  3. Test the complete pipeline"
