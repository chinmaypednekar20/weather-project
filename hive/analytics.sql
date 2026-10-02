-- Spatio-Temporal Big Data Analytics for Extreme Weather Events
-- Hive SQL Analytical Queries over NOAA Dataset in weather_db

USE weather_db;

-- 1. Total event count and records across all years
SELECT 
    event_year,
    COUNT(*) AS total_events,
    SUM(total_fatalities) AS total_fatalities,
    SUM(total_injuries) AS total_injuries,
    ROUND(SUM(total_damage_usd) / 1000000.0, 2) AS total_damage_million_usd
FROM noaa_storm_events
GROUP BY event_year
ORDER BY event_year;

-- 2. Top 10 Most Frequent Extreme Weather Event Types
SELECT 
    event_type,
    COUNT(*) AS frequency,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage_of_total
FROM noaa_storm_events
GROUP BY event_type
ORDER BY frequency DESC
LIMIT 10;

-- 3. Top 10 Most Severely Impacted States by Casualties
SELECT 
    state,
    COUNT(*) AS total_events,
    SUM(total_fatalities) AS fatalities,
    SUM(total_injuries) AS injuries,
    SUM(total_casualties) AS total_casualties
FROM noaa_storm_events
GROUP BY state
ORDER BY total_casualties DESC
LIMIT 10;

-- 4. Top 10 States with Highest Economic Property and Crop Damage
SELECT 
    state,
    ROUND(SUM(damage_property_usd) / 1000000.0, 2) AS property_damage_m_usd,
    ROUND(SUM(damage_crops_usd) / 1000000.0, 2) AS crop_damage_m_usd,
    ROUND(SUM(total_damage_usd) / 1000000.0, 2) AS total_damage_m_usd
FROM noaa_storm_events
GROUP BY state
ORDER BY total_damage_m_usd DESC
LIMIT 10;

-- 5. Seasonal Distribution of Severe Weather Events
SELECT 
    season,
    COUNT(*) AS total_events,
    SUM(total_fatalities) AS fatalities,
    ROUND(SUM(total_damage_usd) / 1000000.0, 2) AS total_damage_m_usd
FROM noaa_storm_events
GROUP BY season
ORDER BY total_events DESC;

-- 6. Month-of-Year Peak Event Analysis
SELECT 
    event_month,
    month_name,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) * 100.0 / 337951.0, 2) AS pct_share
FROM noaa_storm_events
GROUP BY event_month, month_name
ORDER BY event_month;
