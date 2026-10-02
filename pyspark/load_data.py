"""
Module: load_data.py
Description: Initializes SparkSession configured with Hive support and loads raw NOAA data from HDFS.
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import SPARK_APP_NAME, SPARK_MASTER, HDFS_RAW_DIR, HDFS_BASE

from pyspark.sql import SparkSession


def get_spark_session(app_name=SPARK_APP_NAME):
    """
    Creates and returns a SparkSession configured for HDFS access and Hive metastore integration.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(SPARK_MASTER) \
        .config("spark.hadoop.fs.defaultFS", HDFS_BASE) \
        .config("spark.sql.warehouse.dir", f"{HDFS_BASE}/user/hive/warehouse") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_raw_noaa_data(spark, hdfs_path=HDFS_RAW_DIR):
    """
    Loads raw NOAA storm events CSV files directly from HDFS.
    """
    print(f"[PySpark] Reading raw NOAA records from HDFS: {hdfs_path}")
    raw_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(hdfs_path)
    return raw_df


if __name__ == "__main__":
    spark = get_spark_session()
    df = load_raw_noaa_data(spark)
    count = df.count()
    print(f"[PySpark] Successfully loaded {count:,} raw records from HDFS.")
    spark.stop()
