"""
Module: clean_data.py
Description: Cleans, transforms, and normalizes raw NOAA Storm Events data using PySpark.
Converts damage magnitude suffixes, parses dates/timestamps, standardizes coordinates,
and writes the partitioned clean dataset back to HDFS in Parquet format.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import HDFS_RAW_DIR, HDFS_PROCESSED_DIR, PROCESSED_DATA_DIR
from load_data import get_spark_session, load_raw_noaa_data

from pyspark.sql.functions import (
    col, when, regexp_extract, to_timestamp, coalesce, lit, trim,
    upper, concat, lpad, month, year, expr
)
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType
)


def parse_damage_column(col_name):
    """
    Parses NOAA damage expressions like '10.50K', '2.00M', '1.50B' into numeric dollar values.
    Returns a Column expression computing double dollar amounts.
    """
    cleaned_str = upper(trim(col(col_name)))
    num_part = regexp_extract(cleaned_str, r"^([0-9.]+)", 1).cast(DoubleType())

    multiplier = when(cleaned_str.contains("K"), lit(1_000.0)) \
        .when(cleaned_str.contains("M"), lit(1_000_000.0)) \
        .when(cleaned_str.contains("B"), lit(1_000_000_000.0)) \
        .otherwise(lit(1.0))

    return coalesce(num_part * multiplier, lit(0.0))


def clean_noaa_data(df):
    """
    Applies comprehensive data cleaning and transformations to NOAA records.
    """
    print("[PySpark] Transforming and normalizing NOAA storm event records...")

    # Standardize column names to lower case
    for c in df.columns:
        df = df.withColumnRenamed(c, c.upper())

    # Calculate damage fields in USD
    cleaned_df = df.withColumn("DAMAGE_PROPERTY_USD", parse_damage_column("DAMAGE_PROPERTY")) \
                   .withColumn("DAMAGE_CROPS_USD", parse_damage_column("DAMAGE_CROPS")) \
                   .withColumn("TOTAL_DAMAGE_USD", col("DAMAGE_PROPERTY_USD") + col("DAMAGE_CROPS_USD"))

    # Parse casualties
    cleaned_df = cleaned_df.withColumn("INJURIES_DIRECT", coalesce(col("INJURIES_DIRECT").cast(IntegerType()), lit(0))) \
                           .withColumn("INJURIES_INDIRECT", coalesce(col("INJURIES_INDIRECT").cast(IntegerType()), lit(0))) \
                           .withColumn("DEATHS_DIRECT", coalesce(col("DEATHS_DIRECT").cast(IntegerType()), lit(0))) \
                           .withColumn("DEATHS_INDIRECT", coalesce(col("DEATHS_INDIRECT").cast(IntegerType()), lit(0))) \
                           .withColumn("TOTAL_INJURIES", col("INJURIES_DIRECT") + col("INJURIES_INDIRECT")) \
                           .withColumn("TOTAL_FATALITIES", col("DEATHS_DIRECT") + col("DEATHS_INDIRECT")) \
                           .withColumn("TOTAL_CASUALTIES", col("TOTAL_INJURIES") + col("TOTAL_FATALITIES"))

    # Parse begin and end timestamps (e.g. '30-APR-24 20:33:00')
    cleaned_df = cleaned_df.withColumn("BEGIN_TIMESTAMP", to_timestamp(col("BEGIN_DATE_TIME"), "dd-MMM-yy HH:mm:ss")) \
                           .withColumn("END_TIMESTAMP", to_timestamp(col("END_DATE_TIME"), "dd-MMM-yy HH:mm:ss"))

    # Extract temporal fields
    cleaned_df = cleaned_df.withColumn("EVENT_YEAR", coalesce(col("YEAR").cast(IntegerType()), year(col("BEGIN_TIMESTAMP")))) \
                           .withColumn("EVENT_MONTH", month(col("BEGIN_TIMESTAMP"))) \
                           .withColumn("MONTH_NAME", trim(col("MONTH_NAME")))

    # Assign season
    cleaned_df = cleaned_df.withColumn("SEASON",
        when(col("EVENT_MONTH").isin(12, 1, 2), lit("Winter"))
        .when(col("EVENT_MONTH").isin(3, 4, 5), lit("Spring"))
        .when(col("EVENT_MONTH").isin(6, 7, 8), lit("Summer"))
        .when(col("EVENT_MONTH").isin(9, 10, 11), lit("Fall"))
        .otherwise(lit("Unknown"))
    )

    # Coordinates
    cleaned_df = cleaned_df.withColumn("LATITUDE", col("BEGIN_LAT").cast(DoubleType())) \
                           .withColumn("LONGITUDE", col("BEGIN_LON").cast(DoubleType()))

    # Event identification and state
    cleaned_df = cleaned_df.withColumn("EVENT_ID", col("EVENT_ID").cast(LongType())) \
                           .withColumn("EPISODE_ID", col("EPISODE_ID").cast(LongType())) \
                           .withColumn("STATE", trim(upper(col("STATE")))) \
                           .withColumn("EVENT_TYPE", trim(col("EVENT_TYPE"))) \
                           .withColumn("COUNTY_ZONE", trim(col("CZ_NAME"))) \
                           .withColumn("TOR_F_SCALE", trim(col("TOR_F_SCALE")))

    # Select curated clean schema
    final_df = cleaned_df.select(
        "EVENT_ID",
        "EPISODE_ID",
        "EVENT_YEAR",
        "EVENT_MONTH",
        "MONTH_NAME",
        "SEASON",
        "BEGIN_TIMESTAMP",
        "END_TIMESTAMP",
        "STATE",
        "COUNTY_ZONE",
        "EVENT_TYPE",
        "LATITUDE",
        "LONGITUDE",
        "TOTAL_FATALITIES",
        "TOTAL_INJURIES",
        "TOTAL_CASUALTIES",
        "DAMAGE_PROPERTY_USD",
        "DAMAGE_CROPS_USD",
        "TOTAL_DAMAGE_USD",
        "TOR_F_SCALE"
    ).filter(col("EVENT_ID").isNotNull() & col("STATE").isNotNull())

    return final_df


def run_pipeline():
    spark = get_spark_session("NOAA-DataCleaning")
    raw_df = load_raw_noaa_data(spark)
    total_raw = raw_df.count()
    print(f"[PySpark] Total raw events loaded: {total_raw:,}")

    clean_df = clean_noaa_data(raw_df)
    clean_df.cache()

    total_clean = clean_df.count()
    print(f"[PySpark] Total validated clean events: {total_clean:,}")

    print(f"[PySpark] Writing partitioned Parquet dataset to HDFS: {HDFS_PROCESSED_DIR}")
    clean_df.write \
        .mode("overwrite") \
        .partitionBy("EVENT_YEAR") \
        .parquet(HDFS_PROCESSED_DIR)

    print("[PySpark] Data cleaning and HDFS Parquet ingestion complete.")
    spark.stop()


if __name__ == "__main__":
    run_pipeline()
