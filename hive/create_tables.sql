-- Spatio-Temporal Big Data Analytics for Extreme Weather Events
-- Hive Table Definitions
USE weather_db;

DROP TABLE IF EXISTS noaa_storm_events;

CREATE EXTERNAL TABLE IF NOT EXISTS noaa_storm_events (
    event_id BIGINT COMMENT 'Unique event identifier',
    episode_id BIGINT COMMENT 'Episode group identifier',
    event_month INT COMMENT 'Numeric month of event',
    month_name STRING COMMENT 'Full month name',
    season STRING COMMENT 'Season (Winter, Spring, Summer, Fall)',
    begin_timestamp TIMESTAMP COMMENT 'Event start timestamp',
    end_timestamp TIMESTAMP COMMENT 'Event termination timestamp',
    state STRING COMMENT 'US State or territorial region',
    county_zone STRING COMMENT 'County or meteorological zone',
    event_type STRING COMMENT 'Standardized meteorological event type',
    latitude DOUBLE COMMENT 'Geographic latitude coordinate',
    longitude DOUBLE COMMENT 'Geographic longitude coordinate',
    total_fatalities INT COMMENT 'Sum of direct and indirect fatalities',
    total_injuries INT COMMENT 'Sum of direct and indirect injuries',
    total_casualties INT COMMENT 'Combined injuries and fatalities',
    damage_property_usd DOUBLE COMMENT 'Standardized property damage in USD',
    damage_crops_usd DOUBLE COMMENT 'Standardized crop damage in USD',
    total_damage_usd DOUBLE COMMENT 'Total economic damage in USD',
    tor_f_scale STRING COMMENT 'Tornado Fujita/Enhanced-Fujita intensity scale'
)
COMMENT 'Curated NOAA extreme weather events dataset partitioned by year'
PARTITIONED BY (event_year INT)
STORED AS PARQUET
LOCATION '/weather/processed/events';

-- Synchronize metadata with HDFS partition directories
MSCK REPAIR TABLE noaa_storm_events;

SHOW PARTITIONS noaa_storm_events;

SELECT COUNT(*) AS total_records FROM noaa_storm_events;
