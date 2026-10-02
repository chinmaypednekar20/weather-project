"""
Module: impact_analysis.py
Description: Evaluates severe human and economic impacts (fatalities, injuries, property vs. crop damage) using PySpark.
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


def run_impact_analysis(spark=None):
    close_spark = False
    if spark is None:
        spark = get_spark_session("NOAA-ImpactAnalysis")
        close_spark = True

    print(f"[PySpark Impact] Reading cleaned dataset from {HDFS_PROCESSED_DIR}...")
    df = spark.read.parquet(HDFS_PROCESSED_DIR)

    # 1. Casualty Impact by Event Type (Top 15)
    casualty_df = df.groupBy("EVENT_TYPE") \
        .agg(
            count("EVENT_ID").alias("event_count"),
            spark_sum("TOTAL_FATALITIES").alias("fatalities"),
            spark_sum("TOTAL_INJURIES").alias("injuries"),
            spark_sum("TOTAL_CASUALTIES").alias("total_casualties")
        ) \
        .filter(col("total_casualties") > 0) \
        .orderBy(desc("total_casualties"))

    casualty_data = [row.asDict() for row in casualty_df.limit(15).collect()]

    # 2. Economic Damage by Event Type (Property vs. Crop Damage)
    damage_df = df.groupBy("EVENT_TYPE") \
        .agg(
            count("EVENT_ID").alias("event_count"),
            spark_round(spark_sum("DAMAGE_PROPERTY_USD") / 1_000_000.0, 2).alias("property_damage_m_usd"),
            spark_round(spark_sum("DAMAGE_CROPS_USD") / 1_000_000.0, 2).alias("crop_damage_m_usd"),
            spark_round(spark_sum("TOTAL_DAMAGE_USD") / 1_000_000.0, 2).alias("total_damage_m_usd")
        ) \
        .filter(col("total_damage_m_usd") > 0.0) \
        .orderBy(desc("total_damage_m_usd"))

    damage_data = [row.asDict() for row in damage_df.limit(15).collect()]

    # 3. Tornado Intensity Impact (by Fujita/EF Scale)
    tornado_df = df.filter(col("EVENT_TYPE").contains("Tornado") & col("TOR_F_SCALE").isNotNull() & (col("TOR_F_SCALE") != "")) \
        .groupBy("TOR_F_SCALE") \
        .agg(
            count("EVENT_ID").alias("tornado_count"),
            spark_sum("TOTAL_FATALITIES").alias("fatalities"),
            spark_sum("TOTAL_INJURIES").alias("injuries"),
            spark_round(spark_sum("TOTAL_DAMAGE_USD") / 1_000_000.0, 2).alias("damage_m_usd")
        ) \
        .orderBy("TOR_F_SCALE")

    tornado_data = [row.asDict() for row in tornado_df.collect()]

    # 4. Top 10 Most Damaging Single Extreme Events
    top_damaging_df = df.select(
        "EVENT_ID",
        "EVENT_TYPE",
        "STATE",
        "COUNTY_ZONE",
        "EVENT_YEAR",
        "MONTH_NAME",
        "DAMAGE_PROPERTY_USD",
        "DAMAGE_CROPS_USD",
        "TOTAL_DAMAGE_USD",
        "TOTAL_FATALITIES",
        "TOTAL_INJURIES"
    ).orderBy(desc("TOTAL_DAMAGE_USD")).limit(10)

    top_damaging_events = [row.asDict() for row in top_damaging_df.collect()]

    # Write aggregate to HDFS
    hdfs_dest = f"{HDFS_OUTPUT_DIR}/impact"
    print(f"[PySpark Impact] Writing aggregate to HDFS: {hdfs_dest}")
    casualty_df.write.mode("overwrite").parquet(f"{hdfs_dest}/casualty_summary")
    damage_df.write.mode("overwrite").parquet(f"{hdfs_dest}/damage_summary")

    # Export summary for Dashboard consumption
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    summary_path = os.path.join(OUTPUT_DIR, "impact_summary.json")
    output = {
        "casualty_impact": casualty_data,
        "economic_damage": damage_data,
        "tornado_severity": tornado_data,
        "top_damaging_events": top_damaging_events
    }
    with open(summary_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"[PySpark Impact] Saved impact summary to {summary_path}")

    if close_spark:
        spark.stop()
    return output


if __name__ == "__main__":
    run_impact_analysis()
