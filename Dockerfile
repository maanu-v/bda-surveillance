FROM bitnami/spark:3.4.0

USER root

# Install system dependencies for OpenCV & ffmpeg
RUN apt-get update && \
    mkdir -p /var/lib/apt/lists/partial && \
    apt-get install -y libgl1 ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir opencv-python-headless ultralytics kafka-python

USER 1001
