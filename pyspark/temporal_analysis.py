"""
Module: temporal_analysis.py
Description: Performs multi-scale temporal aggregation (monthly, seasonal, year-month cross-tab) using PySpark.
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


def run_temporal_analysis(spark=None):
    close_spark = False
    if spark is None:
        spark = get_spark_session("NOAA-TemporalAnalysis")
        close_spark = True

    print(f"[PySpark Temporal] Reading cleaned dataset from {HDFS_PROCESSED_DIR}...")
    df = spark.read.parquet(HDFS_PROCESSED_DIR)

    # 1. Month-of-Year Distribution (1 to 12)
    monthly_df = df.filter(col("EVENT_MONTH").isNotNull()) \
        .groupBy("EVENT_MONTH", "MONTH_NAME") \
        .agg(
            count("EVENT_ID").alias("event_count"),
            spark_sum("TOTAL_FATALITIES").alias("fatalities"),
            spark_sum("TOTAL_INJURIES").alias("injuries"),
            spark_round(spark_sum("TOTAL_DAMAGE_USD") / 1_000_000.0, 2).alias("damage_m_usd")
        ) \
        .orderBy("EVENT_MONTH")

    monthly_data = [row.asDict() for row in monthly_df.collect()]

    # 2. Seasonal Distribution
    seasonal_df = df.filter(col("SEASON").isNotNull()) \
        .groupBy("SEASON") \
        .agg(
            count("EVENT_ID").alias("event_count"),
            spark_sum("TOTAL_FATALITIES").alias("fatalities"),
            spark_sum("TOTAL_INJURIES").alias("injuries"),
            spark_round(spark_sum("TOTAL_DAMAGE_USD") / 1_000_000.0, 2).alias("damage_m_usd")
        ) \
        .orderBy(desc("event_count"))

    seasonal_data = [row.asDict() for row in seasonal_df.collect()]

    # 3. Year x Month Heatmap Pivot Matrix
    year_month_df = df.filter(col("EVENT_MONTH").isNotNull()) \
        .groupBy("EVENT_YEAR", "EVENT_MONTH", "MONTH_NAME") \
        .agg(count("EVENT_ID").alias("event_count")) \
        .orderBy("EVENT_YEAR", "EVENT_MONTH")

    year_month_data = [row.asDict() for row in year_month_df.collect()]

    # 4. Season by Event Type breakdown
    season_event_df = df.groupBy("SEASON", "EVENT_TYPE") \
        .agg(count("EVENT_ID").alias("count")) \
        .orderBy("SEASON", desc("count"))

    season_event_data = [row.asDict() for row in season_event_df.collect()]

    # Write aggregate to HDFS
    hdfs_dest = f"{HDFS_OUTPUT_DIR}/temporal"
    print(f"[PySpark Temporal] Writing aggregate to HDFS: {hdfs_dest}")
    monthly_df.write.mode("overwrite").parquet(f"{hdfs_dest}/monthly_summary")
    seasonal_df.write.mode("overwrite").parquet(f"{hdfs_dest}/seasonal_summary")

    # Export summary for Dashboard consumption
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    summary_path = os.path.join(OUTPUT_DIR, "temporal_summary.json")
    output = {
        "monthly_patterns": monthly_data,
        "seasonal_patterns": seasonal_data,
        "year_month_matrix": year_month_data,
        "seasonal_event_types": season_event_data
    }
    with open(summary_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"[PySpark Temporal] Saved temporal summary to {summary_path}")

    if close_spark:
        spark.stop()
    return output


if __name__ == "__main__":
    run_temporal_analysis()
