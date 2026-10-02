#!/bin/bash
# Script to verify and start HDFS daemons if not running
source /home/chinmay/bigdata-project/env.sh

echo "=============================================="
echo " Checking HDFS Cluster Daemons...             "
echo "=============================================="

if ! jps | grep -q "NameNode"; then
    echo "[HDFS] Starting HDFS daemons..."
    start-dfs.sh
    sleep 3
else
    echo "[HDFS] NameNode is already running."
fi

jps
hdfs dfs -ls /
echo "[HDFS] Storage cluster verified."
