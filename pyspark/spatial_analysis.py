"""
Module: spatial_analysis.py
Description: Computes spatial distributions, state rankings, and geographic hotspot coordinates using PySpark.
Saves aggregated datasets to HDFS and exports local JSON summaries for dashboard visualization.
"""
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import HDFS_PROCESSED_DIR, HDFS_OUTPUT_DIR, OUTPUT_DIR
from load_data import get_spark_session

from pyspark.sql.functions import col, count, sum as spark_sum, round as spark_round, desc


def run_spatial_analysis(spark=None):
    close_spark = False
    if spark is None:
        spark = get_spark_session("NOAA-SpatialAnalysis")
        close_spark = True

    print(f"[PySpark Spatial] Reading cleaned dataset from {HDFS_PROCESSED_DIR}...")
    df = spark.read.parquet(HDFS_PROCESSED_DIR)

    # 1. State-level aggregated analytics
    state_df = df.groupBy("STATE") \
        .agg(
            count("EVENT_ID").alias("event_count"),
            spark_sum("TOTAL_FATALITIES").alias("fatalities"),
            spark_sum("TOTAL_INJURIES").alias("injuries"),
            spark_sum("TOTAL_CASUALTIES").alias("total_casualties"),
            spark_round(spark_sum("DAMAGE_PROPERTY_USD") / 1_000_000.0, 2).alias("property_damage_m_usd"),
            spark_round(spark_sum("DAMAGE_CROPS_USD") / 1_000_000.0, 2).alias("crop_damage_m_usd"),
            spark_round(spark_sum("TOTAL_DAMAGE_USD") / 1_000_000.0, 2).alias("total_damage_m_usd")
        ) \
        .orderBy(desc("event_count"))

    state_data = [row.asDict() for row in state_df.collect()]

    # 2. State-by-EventType breakdown (top pairs)
    state_event_df = df.groupBy("STATE", "EVENT_TYPE") \
        .agg(count("EVENT_ID").alias("count")) \
        .orderBy("STATE", desc("count"))

    state_event_data = [row.asDict() for row in state_event_df.collect()]

    # 3. Geospatial coordinates for Hotspot Map (continental US filtering)
    # Valid continental latitude ~ 24 to 50, longitude ~ -125 to -66
    geo_df = df.filter(
        col("LATITUDE").isNotNull() &
        col("LONGITUDE").isNotNull() &
        (col("LATITUDE").between(24.0, 50.0)) &
        (col("LONGITUDE").between(-125.0, -66.0))
    ).select(
        "EVENT_ID",
        "EVENT_TYPE",
        "STATE",
        "EVENT_YEAR",
        "LATITUDE",
        "LONGITUDE",
        "TOTAL_FATALITIES",
        "TOTAL_INJURIES",
        "TOTAL_DAMAGE_USD"
    )

    # Sample representative events for clean map visualization (~10,000 points)
    total_geo = geo_df.count()
    fraction = min(1.0, 10000.0 / max(1, total_geo))
    sampled_geo = geo_df.sample(withReplacement=False, fraction=fraction, seed=42)
    hotspot_points = [row.asDict() for row in sampled_geo.limit(10000).collect()]

    # Write aggregate to HDFS
    hdfs_dest = f"{HDFS_OUTPUT_DIR}/spatial"
    print(f"[PySpark Spatial] Writing aggregate to HDFS: {hdfs_dest}")
    state_df.write.mode("overwrite").parquet(f"{hdfs_dest}/state_summary")

    # Export summary for Dashboard consumption
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    summary_path = os.path.join(OUTPUT_DIR, "spatial_summary.json")
    output = {
        "state_rankings": state_data,
        "state_event_types": state_event_data,
        "hotspot_sample_points": hotspot_points
    }
    with open(summary_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"[PySpark Spatial] Saved spatial summary ({len(state_data)} states, {len(hotspot_points)} hotspot points) to {summary_path}")

    if close_spark:
        spark.stop()
    return output


if __name__ == "__main__":
    run_spatial_analysis()
