"""
Data loader module for Extreme Weather Analytics Dashboard.
Loads pre-aggregated PySpark output datasets and live stream buffers with caching.
"""
import os
import json
import pandas as pd
import streamlit as st

PROJECT_ROOT = "/home/chinmay/bigdata-project/weather-project"
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
STREAM_BUFFER_FILE = os.path.join(PROJECT_ROOT, "data", "streaming", "live_stream_buffer.json")
STREAM_METRICS_FILE = os.path.join(PROJECT_ROOT, "output", "stream_metrics.json")


@st.cache_data(ttl=60)
def load_historical_data():
    fpath = os.path.join(OUTPUT_DIR, "historical_summary.json")
    if not os.path.exists(fpath):
        return {}, pd.DataFrame(), pd.DataFrame()
    with open(fpath, "r") as f:
        data = json.load(f)
    yearly_df = pd.DataFrame(data.get("yearly_trends", []))
    event_type_df = pd.DataFrame(data.get("top_event_types", []))
    yearly_events_df = pd.DataFrame(data.get("yearly_event_types", []))
    return data, yearly_df, event_type_df, yearly_events_df


@st.cache_data(ttl=60)
def load_spatial_data():
    fpath = os.path.join(OUTPUT_DIR, "spatial_summary.json")
    if not os.path.exists(fpath):
        return {}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    with open(fpath, "r") as f:
        data = json.load(f)
    state_df = pd.DataFrame(data.get("state_rankings", []))
    state_event_df = pd.DataFrame(data.get("state_event_types", []))
    hotspots_df = pd.DataFrame(data.get("hotspot_sample_points", []))
    return data, state_df, state_event_df, hotspots_df


@st.cache_data(ttl=60)
def load_temporal_data():
    fpath = os.path.join(OUTPUT_DIR, "temporal_summary.json")
    if not os.path.exists(fpath):
        return {}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    with open(fpath, "r") as f:
        data = json.load(f)
    monthly_df = pd.DataFrame(data.get("monthly_patterns", []))
    seasonal_df = pd.DataFrame(data.get("seasonal_patterns", []))
    matrix_df = pd.DataFrame(data.get("year_month_matrix", []))
    return data, monthly_df, seasonal_df, matrix_df


@st.cache_data(ttl=60)
def load_impact_data():
    fpath = os.path.join(OUTPUT_DIR, "impact_summary.json")
    if not os.path.exists(fpath):
        return {}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    with open(fpath, "r") as f:
        data = json.load(f)
    casualty_df = pd.DataFrame(data.get("casualty_impact", []))
    damage_df = pd.DataFrame(data.get("economic_damage", []))
    tornado_df = pd.DataFrame(data.get("tornado_severity", []))
    top_events_df = pd.DataFrame(data.get("top_damaging_events", []))
    return data, casualty_df, damage_df, tornado_df, top_events_df


def load_streaming_buffer():
    if not os.path.exists(STREAM_BUFFER_FILE):
        return [], {}
    try:
        with open(STREAM_BUFFER_FILE, "r") as f:
            events = json.load(f)
    except Exception:
        events = []

    metrics = {}
    if os.path.exists(STREAM_METRICS_FILE):
        try:
            with open(STREAM_METRICS_FILE, "r") as f:
                metrics = json.load(f)
        except Exception:
            metrics = {}

    return events, metrics
