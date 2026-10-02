# Spatio-Temporal Big Data Analytics for Extreme Weather Events using NOAA Data

**Application Name:** Extreme Weather Analytics System  
**Course Project:** College Big Data Analytics Mini-Project  
**Environment:** Linux (Ubuntu under Windows WSL2)  
**Primary Stack:** Apache Hadoop HDFS 3.5.0, Apache Hive 4.2.1, Apache Spark 4.2.0, Apache Flume 1.11.0, PySpark, Streamlit, Python 3  

## 1. Problem Statement & Objective

Extreme meteorological events (such as tornadoes, severe thunderstorm winds, flash floods, blizzards, hail, and excessive heat) cause significant human casualties and billions of dollars in economic damage annually. Traditional relational databases struggle to efficiently process and analyze multi-year, multi-gigabyte spatial and temporal records across hundreds of thousands of events.

The objective of this project is to build an end-to-end Big Data system that ingests, cleans, processes, and visualizes large-scale historical and streaming extreme-weather data from the official National Oceanic and Atmospheric Administration (NOAA) database.

The project answers the fundamental question:  
**"What patterns, geographic hotspots, temporal cycles, and severity distributions can be extracted from large amounts of historical and incoming extreme weather data?"**

> **Note:** This project is an analytical Big Data system, **not** a weather forecasting or machine learning prediction model.

---

## 2. Dataset Information

The system uses genuine historical data from the **NOAA National Centers for Environmental Information (NCEI) Storm Events Database**:
- **Dataset Files:** `StormEvents_details-ftp_v1.0_d[2020-2024]_c*.csv.gz`
- **Time Horizon:** 2020 to 2024 (5 complete years)
- **Total Validated Records:** **337,951 real meteorological events**
- **Geographic Coverage:** All 50 US States and territorial zones
- **Key Fields Utilized:**
  - `EVENT_ID`, `EPISODE_ID`: Unique event identifiers
  - `EVENT_TYPE`: Standardized event classification (Thunderstorm Wind, Hail, Tornado, Flash Flood, etc.)
  - `STATE`, `CZ_NAME`: Geographic state and county/zone
  - `BEGIN_DATE_TIME`, `END_DATE_TIME`: Timestamps
  - `BEGIN_LAT`, `BEGIN_LON`: Coordinates for geospatial hotspot mapping
  - `DEATHS_DIRECT`, `DEATHS_INDIRECT`: Fatalities
  - `INJURIES_DIRECT`, `INJURIES_INDIRECT`: Injuries
  - `DAMAGE_PROPERTY`, `DAMAGE_CROPS`: Dollar values with magnitude suffixes (`K` for thousands, `M` for millions, `B` for billions)

---

## 3. Technology Stack & Architectural Roles

| Component | Version | Role in Architecture |
| :--- | :--- | :--- |
| **Hadoop HDFS** | 3.5.0 | Distributed, fault-tolerant raw and processed storage cluster (`hdfs://localhost:9000`) |
| **Apache Hive** | 4.2.1 | SQL data warehouse engine for structured querying over partitioned Parquet datasets (`weather_db`) |
| **Apache Spark / PySpark** | 4.2.0 | In-memory distributed data processing, data cleaning, and spatio-temporal aggregation engine |
| **Apache Flume** | 1.11.0 | Event-driven streaming ingestion agent capturing incoming socket events into HDFS |
| **Streamlit** | 1.35+ | Interactive web dashboard presenting analytical findings, maps, and live feeds |
| **Python** | 3.14 / 3.x | Glue code, PySpark ETL scripts, and simulated stream generator |

---

## 4. System Architecture

```
                      +---------------------------------------+
                      |   NOAA Storm Events Database (Gov)   |
                      |   (Real historical CSV data files)    |
                      +---------------------------------------+
                                          |
                        +-----------------+-----------------+
                        | (Batch Historical)                | (Simulated Streaming Replay)
                        v                                   v
             +---------------------+               +------------------+
             | HDFS Ingestion      |               | Python Replay    |
             | /weather/raw/noaa/  |               | Script           |
             +---------------------+               +------------------+
                        |                                   |
           +------------+------------+                      v
           |                         |             +------------------+
           v                         v             | Apache Flume     |
   +---------------+        +----------------+     | Agent            |
   | Apache Hive   |        | Apache Spark / |     +------------------+
   | (weather_db)  |        | PySpark Engine |              |
   | - SQL queries |        +----------------+              v
   | - Schemas     |                 |             +------------------+
   +---------------+                 v             | HDFS Streaming   |
           |             +-----------------------+ | /weather/stream/ |
           |             | Spatio-Temporal &     | +------------------+
           |             | Impact Analytics      |          |
           |             +-----------------------+          v
           |                         |             +------------------+
           +------------+------------+             | Stream Processor |
                        |                          +------------------+
                        v                                   |
             +---------------------+                        |
             | Aggregated Outputs  |<-----------------------+
             | (HDFS / Dashboard)  |
             +---------------------+
                        |
                        v
             +---------------------+
             | Streamlit Dashboard |
             | (Academic & Clean)  |
             +---------------------+
```

---

## 5. HDFS Directory Hierarchy

The distributed filesystem is structured as follows:
```
hdfs://localhost:9000/
└── weather/
    ├── raw/
    │   └── noaa/                   <-- Uploaded raw compressed NOAA CSV.GZ files
    ├── processed/
    │   └── events/                 <-- Cleaned Parquet records partitioned by year
    │       ├── EVENT_YEAR=2020/
    │       ├── EVENT_YEAR=2021/
    │       ├── EVENT_YEAR=2022/
    │       ├── EVENT_YEAR=2023/
    │       └── EVENT_YEAR=2024/
    ├── streaming/                  <-- Ingestion target sink for Apache Flume
    └── output/                     <-- Aggregated Big Data analytical results
        ├── historical/
        ├── spatial/
        ├── temporal/
        └── impact/
```

## 6. Step-by-Step Setup and Execution

### Step 1: Environment Variables
Ensure the environment variables are active in your shell:
```bash
source ~/bigdata-project/env.sh
```

### Step 2: Start HDFS Services
Verify that the NameNode and DataNode are running:
```bash
~/bigdata-project/weather-project/scripts/start_hdfs.sh
```
Check with `jps`. You should see `NameNode`, `DataNode`, and `SecondaryNameNode`.

### Step 3: Run the Full Data Pipeline
Run the end-to-end PySpark and Hive processing pipeline:
```bash
~/bigdata-project/weather-project/scripts/run_pipeline.sh
```
This script automatically:
1. Ingests raw NOAA datasets into HDFS `/weather/raw/noaa/`
2. Cleans and normalizes all 337,951 records with PySpark
3. Writes partitioned Parquet files to `/weather/processed/events/`
4. Creates the Hive `weather_db.noaa_storm_events` table and repairs partitions
5. Executes all four distributed PySpark analytics pipelines (Historical, Spatial, Temporal, Impact)

### Step 4: Run Hive Queries Directly
To inspect queries directly using Hive Beeline:
```bash
hive -u 'jdbc:hive2://' -f ~/bigdata-project/weather-project/hive/analytics.sql
```

### Step 5: Launch the Streamlit Dashboard
Launch the web interface:
```bash
~/bigdata-project/weather-project/scripts/start_dashboard.sh
```
Open your browser and navigate to:
```
http://localhost:8501
```

### Step 6: Test Simulated Streaming Ingestion
In a separate terminal, trigger the simulated real-time stream replay:
```bash
python3 ~/bigdata-project/weather-project/streaming/replay_noaa.py 50 0.1
python3 ~/bigdata-project/weather-project/streaming/process_stream.py
```
View the incoming events live in the **Simulated Real-Time Monitoring** tab on the dashboard.

---

## 7. Key Analytical Findings

1. **Longitudinal Volume:** An average of ~67,500 severe weather events occur annually in the United States, peaking in 2023 with 75,593 events.
2. **Dominant Categories:** Thunderstorm Wind (104,260 events), Hail (47,402 events), and High Wind (42,674 events) account for over 57% of all recorded extreme weather occurrences.
3. **Casualty Leaders:** Excessive Heat and Tornadoes represent the leading causes of human fatalities and severe injuries, despite lower event counts compared to wind events.
4. **Economic Loss Leaders:** Severe Floods and Tornadoes generate the highest property losses, while Hail and Severe Drought generate the highest crop damage.
5. **Seasonal Cycles:** Spring (March–May) shows the sharpest spike in severe convective storms and tornadoes, while Summer experiences the highest concentration of wind and flash-flood events.

---

## 8. Limitations & Real-World Live Streaming

- **Simulated Feed:** Real-time monitoring replays official historical NOAA records with realistic intervals. Connecting to a genuine live NOAA API requires NOAA Weather Wire Service (NWWS) satellite receivers or authenticated NWS API polling.
- **Single-Node Development Mode:** The cluster is configured as a single-node WSL2 development cluster. In a multi-node production deployment, replication factor would be set to 3 (`dfs.replication=3`), and YARN would manage resource scheduling across multiple worker nodes.
