"""
Module: historical_analysis.py
Description: Performs multi-year historical event trend analysis and event-type frequency breakdown using PySpark.
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


def run_historical_analysis(spark=None):
    close_spark = False
    if spark is None:
        spark = get_spark_session("NOAA-HistoricalAnalysis")
        close_spark = True

    print(f"[PySpark Historical] Reading cleaned dataset from {HDFS_PROCESSED_DIR}...")
    df = spark.read.parquet(HDFS_PROCESSED_DIR)

    # 1. Yearly Event Trends
    yearly_df = df.groupBy("EVENT_YEAR") \
        .agg(
            count("EVENT_ID").alias("total_events"),
            spark_sum("TOTAL_FATALITIES").alias("fatalities"),
            spark_sum("TOTAL_INJURIES").alias("injuries"),
            spark_round(spark_sum("TOTAL_DAMAGE_USD") / 1_000_000.0, 2).alias("damage_m_usd")
        ) \
        .orderBy("EVENT_YEAR")

    yearly_data = [row.asDict() for row in yearly_df.collect()]

    # 2. Event Type Frequency Distribution (Top 20)
    event_type_df = df.groupBy("EVENT_TYPE") \
        .agg(
            count("EVENT_ID").alias("event_count"),
            spark_sum("TOTAL_FATALITIES").alias("fatalities"),
            spark_sum("TOTAL_INJURIES").alias("injuries"),
            spark_round(spark_sum("TOTAL_DAMAGE_USD") / 1_000_000.0, 2).alias("damage_m_usd")
        ) \
        .orderBy(desc("event_count"))

    event_type_data = [row.asDict() for row in event_type_df.limit(20).collect()]

    # 3. Yearly Event Type Breakdown
    yearly_event_df = df.groupBy("EVENT_YEAR", "EVENT_TYPE") \
        .agg(count("EVENT_ID").alias("count")) \
        .orderBy("EVENT_YEAR", desc("count"))

    yearly_event_data = [row.asDict() for row in yearly_event_df.collect()]

    # Write aggregate to HDFS
    hdfs_dest = f"{HDFS_OUTPUT_DIR}/historical"
    print(f"[PySpark Historical] Writing aggregate to HDFS: {hdfs_dest}")
    yearly_df.write.mode("overwrite").parquet(f"{hdfs_dest}/yearly_summary")
    event_type_df.write.mode("overwrite").parquet(f"{hdfs_dest}/event_type_summary")

    # Export summary for Dashboard consumption
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    summary_path = os.path.join(OUTPUT_DIR, "historical_summary.json")
    output = {
        "yearly_trends": yearly_data,
        "top_event_types": event_type_data,
        "yearly_event_types": yearly_event_data
    }
    with open(summary_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"[PySpark Historical] Saved summary to {summary_path}")

    if close_spark:
        spark.stop()
    return output


if __name__ == "__main__":
    run_historical_analysis()
