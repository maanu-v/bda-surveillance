# Wildtrack Dataset Structure

This directory should contain the **Wildtrack Multi-Camera Person Dataset** (~10GB).

## Expected Structure

```
data/wildtrack/
├── Image_subsets/          # Images from 7 cameras
│   ├── C1/                # Camera 1 images
│   ├── C2/                # Camera 2 images
│   ├── C3/                # Camera 3 images
│   ├── C4/                # Camera 4 images
│   ├── C5/                # Camera 5 images
│   ├── C6/                # Camera 6 images
│   └── C7/                # Camera 7 images
├── annotations_positions/  # Frame annotations (JSON)
├── calibrations/          # Camera calibration files
│   ├── extrinsic/        # Extrinsic camera parameters
│   ├── intrinsic_zero/   # Intrinsic calibrations (undistorted)
│   └── intrinsic_original/ # Original intrinsic calibrations
├── cam1.mp4              # Video from camera 1
├── cam2.mp4              # Video from camera 2
├── cam3.mp4              # Video from camera 3
├── cam4.mp4              # Video from camera 4
├── cam5.mp4              # Video from camera 5
├── cam6.mp4              # Video from camera 6
├── cam7.mp4              # Video from camera 7
└── rectangles.pom        # Rectangle ID to bounding box mapping
```

## Dataset Information

- **Source**: EPFL Wildtrack Dataset (Reduced from 62GB to ~10GB for Kaggle)
- **Cameras**: 7 overlapping cameras
- **Frames**: >40,000 synchronized frames
- **Grid**: 480x1440 with 2.5cm spacing
- **Origin**: (-3.0, -9.0)

## 3D Coordinate Conversion

```python
# Convert positionID to 3D coordinates
X = -3.0 + 0.025 * (position_id % 480)
Y = -9.0 + 0.025 * (position_id // 480)
```

## Usage

1. **Download** the Wildtrack dataset from Kaggle
2. **Extract** all files to this directory
3. **Verify** the structure matches the above layout
4. **Run** Kafka producers to simulate camera streams

## Citation

If you use this dataset, please cite:
```
"The WILDTRACK Multi-Camera Person Dataset."
T. Chavdarova et al.
https://arxiv.org/pdf/1707.09299.pdf
```
