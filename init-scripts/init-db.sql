-- Initialize the surveillance database
CREATE SCHEMA IF NOT EXISTS surveillance;

-- Table for storing person embeddings
CREATE TABLE IF NOT EXISTS surveillance.person_embeddings (
    id SERIAL PRIMARY KEY,
    global_person_id VARCHAR(50) NOT NULL,
    camera_id VARCHAR(20) NOT NULL,
    local_track_id INTEGER NOT NULL,
    embedding FLOAT[] NOT NULL,
    confidence FLOAT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    bounding_box JSONB,
    frame_path VARCHAR(255),
    UNIQUE(global_person_id, camera_id, local_track_id, timestamp)
);

-- Table for storing trajectories
CREATE TABLE IF NOT EXISTS surveillance.trajectories (
    id SERIAL PRIMARY KEY,
    global_person_id VARCHAR(50) NOT NULL,
    camera_id VARCHAR(20) NOT NULL,
    entry_time TIMESTAMP NOT NULL,
    exit_time TIMESTAMP,
    path_points JSONB, -- Array of {x, y, timestamp} points
    confidence_score FLOAT NOT NULL,
    status VARCHAR(20) DEFAULT 'active' -- active, completed, lost
);

-- Table for storing detection events
CREATE TABLE IF NOT EXISTS surveillance.detections (
    id SERIAL PRIMARY KEY,
    camera_id VARCHAR(20) NOT NULL,
    local_track_id INTEGER NOT NULL,
    global_person_id VARCHAR(50),
    detection_time TIMESTAMP NOT NULL,
    bounding_box JSONB NOT NULL, -- {x, y, width, height}
    confidence FLOAT NOT NULL,
    frame_path VARCHAR(255),
    processed BOOLEAN DEFAULT FALSE
);

-- Table for camera metadata
CREATE TABLE IF NOT EXISTS surveillance.cameras (
    camera_id VARCHAR(20) PRIMARY KEY,
    location JSONB, -- {lat, lng, description}
    status VARCHAR(20) DEFAULT 'active',
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_embeddings_global_id ON surveillance.person_embeddings(global_person_id);
CREATE INDEX IF NOT EXISTS idx_embeddings_camera ON surveillance.person_embeddings(camera_id);
CREATE INDEX IF NOT EXISTS idx_embeddings_timestamp ON surveillance.person_embeddings(timestamp);
CREATE INDEX IF NOT EXISTS idx_trajectories_global_id ON surveillance.trajectories(global_person_id);
CREATE INDEX IF NOT EXISTS idx_detections_camera_time ON surveillance.detections(camera_id, detection_time);
CREATE INDEX IF NOT EXISTS idx_detections_global_id ON surveillance.detections(global_person_id);

-- Insert sample camera data
INSERT INTO surveillance.cameras (camera_id, location) VALUES 
('cam_001', '{"lat": 40.7128, "lng": -74.0060, "description": "Main Entrance"}'),
('cam_002', '{"lat": 40.7130, "lng": -74.0058, "description": "Corridor A"}'),
('cam_003', '{"lat": 40.7125, "lng": -74.0062, "description": "Exit Gate"}'),
('cam_004', '{"lat": 40.7132, "lng": -74.0055, "description": "Parking Area"}'),
('cam_005', '{"lat": 40.7127, "lng": -74.0065, "description": "Central Plaza"}'),
('cam_006', '{"lat": 40.7135, "lng": -74.0050, "description": "North Wing"}')
ON CONFLICT (camera_id) DO NOTHING;
