"""
Configuration settings for NOAA Spatio-Temporal Big Data Analytics Project.
"""
import os

# Project root directory
PROJECT_ROOT = "/home/chinmay/bigdata-project/weather-project"

# HDFS base and paths
HDFS_BASE = "hdfs://localhost:9000"
HDFS_RAW_DIR = f"{HDFS_BASE}/weather/raw/noaa"
HDFS_PROCESSED_DIR = f"{HDFS_BASE}/weather/processed/events"
HDFS_STREAMING_DIR = f"{HDFS_BASE}/weather/streaming"
HDFS_OUTPUT_DIR = f"{HDFS_BASE}/weather/output"

# Local directories
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
STREAMING_DATA_DIR = os.path.join(DATA_DIR, "streaming")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

# Spark settings
SPARK_APP_NAME = "NOAA-Weather-Analytics"
SPARK_MASTER = "local[2]"
SPARK_DRIVER_MEMORY = "2g"
