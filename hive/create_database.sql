-- Spatio-Temporal Big Data Analytics for Extreme Weather Events
-- Hive Database Initialization
CREATE DATABASE IF NOT EXISTS weather_db
COMMENT 'Database for NOAA extreme weather events spatio-temporal analytics'
LOCATION '/user/hive/warehouse/weather_db.db';

USE weather_db;
SHOW DATABASES;
