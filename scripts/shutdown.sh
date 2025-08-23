#!/bin/bash

# BDA Surveillance System Teardown Script
echo "🛑 Shutting down BDA Surveillance System..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Stop all containers
print_status "Stopping all containers..."
docker compose down

# Optional: Remove volumes (uncomment if you want to clean up data)
# print_warning "Removing all data volumes..."
# docker volume prune -f

print_status "System shutdown complete! 🏁"
echo
echo "To completely clean up (including data volumes), run:"
echo "  docker volume prune -f"
