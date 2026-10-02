"""
Master PySpark Analytics Orchestrator:
Executes historical, spatial, temporal, and impact analytics in a single unified Spark session.
"""
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from load_data import get_spark_session
from historical_analysis import run_historical_analysis
from spatial_analysis import run_spatial_analysis
from temporal_analysis import run_temporal_analysis
from impact_analysis import run_impact_analysis


def run_all():
    print("==================================================")
    print(" NOAA EXTREME WEATHER SPATIO-TEMPORAL ANALYTICS   ")
    print(" Executing PySpark Distributed Processing Engine  ")
    print("==================================================")
    start_time = time.time()

    spark = get_spark_session("NOAA-SpatioTemporal-Master")

    print("\n--- 1/4: Running Historical Analytics ---")
    run_historical_analysis(spark)

    print("\n--- 2/4: Running Spatial Analytics ---")
    run_spatial_analysis(spark)

    print("\n--- 3/4: Running Temporal Analytics ---")
    run_temporal_analysis(spark)

    print("\n--- 4/4: Running Impact & Severity Analytics ---")
    run_impact_analysis(spark)

    spark.stop()
    elapsed = time.time() - start_time
    print("\n==================================================")
    print(f" All Big Data analytics completed in {elapsed:.2f} seconds.")
    print(" Results written to HDFS /weather/output/ and local /output/")
    print("==================================================")


if __name__ == "__main__":
    run_all()
