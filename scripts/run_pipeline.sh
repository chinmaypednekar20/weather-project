#!/bin/bash
# End-to-End Big Data Analytics Pipeline Runner
set -e

source /home/chinmay/bigdata-project/env.sh
PROJECT_DIR="/home/chinmay/bigdata-project/weather-project"

echo "=========================================================="
echo " NOAA Extreme Weather Spatio-Temporal Analytics Pipeline  "
echo "=========================================================="

# 1. Check HDFS
echo -e "\n[Step 1/5] Checking HDFS connectivity..."
hdfs dfs -test -d /weather || hdfs dfs -mkdir -p /weather/raw/noaa /weather/processed /weather/streaming /weather/output

# 2. Check Raw Data in HDFS
echo -e "\n[Step 2/5] Verifying Raw NOAA Ingestion in HDFS..."
hdfs dfs -ls -h /weather/raw/noaa/

# 3. Execute PySpark Data Cleaning and Parquet Partitioning
echo -e "\n[Step 3/5] Executing PySpark Cleaning & Normalization..."
python3 "$PROJECT_DIR/pyspark/clean_data.py"

# 4. Synchronize Hive Metastore and Table Partitions
echo -e "\n[Step 4/5] Synchronizing Hive Metastore Table & Partitions..."
hive -u 'jdbc:hive2://' -f "$PROJECT_DIR/hive/create_tables.sql"

# 5. Execute PySpark Historical, Spatial, Temporal, Impact Analytics
echo -e "\n[Step 5/5] Executing PySpark Distributed Analytics..."
python3 "$PROJECT_DIR/pyspark/run_all_analytics.py"

echo -e "\n=========================================================="
echo " Pipeline execution finished successfully!               "
echo "=========================================================="
