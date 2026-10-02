In Big Data, there are two distinct paradigms. Our project implements **both**:

```
                       NOAA RAW RECORDS
                              │
               ┌──────────────┴──────────────┐
               ▼                             ▼
       [BATCH PIPELINE]             [STREAMING PIPELINE]
   Processes 337,951 records        Replays records sequentially
   in one large distributed job.    one-by-one with realistic delay.
               │                             │
               ▼                             ▼
       HDFS Storage (/raw/)           Python Replay Script
               │                             │
               ▼                             ▼
      PySpark In-Memory ETL          Apache Flume Netcat Agent
               │                             │
               ▼                             ▼
   Partitioned Parquet (/processed/)  HDFS Streaming Sink (/streaming/)
               │                             │
               ▼                             ▼
    Hive SQL Data Warehouse         PySpark Micro-Batch Aggregator
               │                             │
               └──────────────┬──────────────┘
                              ▼
                     STREAMLIT DASHBOARD
```