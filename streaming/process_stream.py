"""
Module: process_stream.py
Description: Processes streaming weather events arriving in HDFS or buffer using PySpark.
Aggregates rolling metrics: active alert states, event type breakdown, and severe event counts.
"""
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SPARK_HOME = os.environ.get("SPARK_HOME", "/home/chinmay/bigdata-project/spark-4.2.0")
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
sys.path.insert(0, os.path.join(SPARK_HOME, "python", "lib", "py4j-0.10.9.9-src.zip"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, desc

PROJECT_ROOT = "/home/chinmay/bigdata-project/weather-project"
BUFFER_FILE = os.path.join(PROJECT_ROOT, "data", "streaming", "live_stream_buffer.json")
STREAM_SUMMARY_FILE = os.path.join(PROJECT_ROOT, "output", "stream_processed_summary.json")


def process_stream_batch():
    if not os.path.exists(BUFFER_FILE):
        print("[Stream Processor] No live streaming events buffer found.")
        return

    spark = SparkSession.builder \
        .appName("NOAA-StreamProcessor") \
        .master("local[2]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("[Stream Processor] Reading live buffer events into PySpark DataFrame...")
    local_path = f"file://{os.path.abspath(BUFFER_FILE)}"
    df = spark.read.option("multiline", "true").json(local_path)

    if df.count() == 0:
        print("[Stream Processor] Buffer is empty.")
        spark.stop()
        return

    # Compute rolling aggregates
    event_type_df = df.groupBy("event_type").agg(count("event_id").alias("count")).orderBy(desc("count"))
    state_df = df.groupBy("state").agg(count("event_id").alias("count")).orderBy(desc("count"))
    total_fatalities = df.agg(spark_sum("fatalities")).collect()[0][0] or 0
    total_injuries = df.agg(spark_sum("injuries")).collect()[0][0] or 0

    event_type_summary = [r.asDict() for r in event_type_df.collect()]
    state_summary = [r.asDict() for r in state_df.collect()]

    summary = {
        "recent_event_count": df.count(),
        "total_fatalities": int(total_fatalities),
        "total_injuries": int(total_injuries),
        "event_types": event_type_summary,
        "active_states": state_summary
    }

    with open(STREAM_SUMMARY_FILE, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"[Stream Processor] Saved processed stream aggregates ({df.count()} events) to {STREAM_SUMMARY_FILE}")
    spark.stop()


if __name__ == "__main__":
    process_stream_batch()
