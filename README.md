# Pose Estimation Pipeline

## Overview

This project implements a modular video processing pipeline for human pose estimation using YOLOv11. It includes tools for data collection, local storage, structured metadata management via PostgreSQL, and post-processing using a pre-trained pose estimation model. The codebase is designed for reproducibility, extensibility, and integration into larger machine learning workflows.

## Key Features

- Command-line interface for participant-driven video collection
- Modular recording system with support for multiple activity types
- Local video and metadata storage
- PostgreSQL-backed metadata persistence
- Batch processing of stored videos via Dagster pipeline
- Pose estimation using Ultralytics YOLO11n-pose
- Structured output in Parquet format

## Technologies Used

- Python 3.10+
- OpenCV (video capture and processing)
- Ultralytics YOLO11n-pose (pose estimation)
- Dagster (pipeline orchestration)
- PostgreSQL (relational database)
- Pandas and NumPy (data handling)

## Directory Structure

```
pose_estimator/
├── src/
│   ├── main.py                  # Entry point for video collection
│   ├── recorder.py              # Webcam video capture abstraction
│   ├── session_manager.py       # Orchestrates recording and saving sessions
│   ├── db_manager.py            # Manages database interactions
│   ├── storage_manager.py       # Handles file system I/O
│   ├── postprocessor.py         # YOLO pose inference and result transformation
│   └── pipeline.py              # Dagster-based batch processing workflow
```

## Setup Instructions

1. **Clone Repository and Install Dependencies**

```bash
git clone https://github.com/YOUR_USERNAME/pose_estimator.git
cd pose_estimator
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

2. **Environment Configuration**

Create a `.env` file in the root directory:

```
PG_HOST=localhost
PG_PORT=5432
PG_USER=postgres
PG_DBNAME=your_db
PG_PASS=your_password
```

3. **Run CLI Video Recorder**

```bash
python -m src.main
```

4. **Run Pipeline**

```bash
dagster job execute -f src/pipeline.py -j video_processing_job --config run_config.yaml
```

5. **Run Tests**

```bash
pytest
```

## PostgreSQL Schema Summary

- **participants**: participant\_name
- **activities**: activity\_name
- **sessions**: session\_start
- **recordings**: video\_path, fps, start\_time, end\_time, duration\_in\_sec, is\_corrupted, foreign keys to session, activity, participant
- **processors**: processor\_name
- **results**: file\_location, foreign keys to recording and processor

## Pipeline Description

The Dagster pipeline performs the following:

1. Retrieves video metadata in a specified time range
2. Loads videos from storage
3. Applies YOLO-based pose estimation
4. Converts results to a structured DataFrame
5. Saves results to Parquet
6. Updates the database with result file paths

## Usage Example

1. Record a 30-second "A-pose" video using CLI
2. Choose to save the recording
3. Process stored videos using pipeline

## Notes

- Project assumes write access to local filesystem and an active PostgreSQL instance

## Author

Anastasia Lurye

