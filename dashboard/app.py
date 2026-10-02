"""
Streamlit Web Dashboard: Extreme Weather Analytics System
Academic Big Data Analytics Application for NOAA Extreme Weather Events.
Demonstrates HDFS storage, Hive SQL analytics, PySpark distributed computing, and Flume streaming.
"""
import streamlit as st
import pandas as pd
import time
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_loader import (
    load_historical_data, load_spatial_data, load_temporal_data,
    load_impact_data, load_streaming_buffer
)
from charts import (
    plot_yearly_trend, plot_event_types, plot_us_map,
    plot_state_ranking, plot_monthly_distribution,
    plot_seasonal_distribution, plot_year_month_heatmap,
    plot_casualty_breakdown, plot_economic_damage
)

st.set_page_config(
    page_title="Extreme Weather Analytics System",
    page_icon="⛅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Clean CSS (Academic / Professional Style)
st.markdown("""
<style>
    .main-header {
        font-size: 1.8rem;
        font-weight: 700;
        color: #2C3E50;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.0rem;
        color: #5D6D7E;
        margin-bottom: 1.5rem;
    }
    .metric-box {
        background-color: #F8F9FA;
        border: 1px solid #E2E8F0;
        border-radius: 6px;
        padding: 12px 16px;
        margin-bottom: 10px;
    }
    .metric-value {
        font-size: 1.5rem;
        font-weight: 700;
        color: #2C3E50;
    }
    .metric-label {
        font-size: 0.85rem;
        color: #7F8C8D;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .tech-pill {
        display: inline-block;
        background-color: #EAEDED;
        color: #2C3E50;
        padding: 3px 8px;
        border-radius: 4px;
        font-size: 0.8rem;
        font-weight: 600;
        margin-right: 6px;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar Navigation
st.sidebar.markdown("### Extreme Weather Analytics")
st.sidebar.markdown("---")

nav_choice = st.sidebar.radio(
    "Navigation Menu",
    [
        "System Overview",
        "Historical Analysis",
        "Spatial Analysis",
        "Temporal Analysis",
        "Impact & Severity Analysis",
        "Simulated Real-Time Monitoring"
    ]
)

st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Dataset:** Official NOAA Storm Events<br>"
    "**Period:** 2020 – 2024 (5 Years)<br>"
    "**Dataset Size:** 337,951 Real Events<br>"
    "**Cluster:** Single-Node WSL2 Hadoop HDFS",
    unsafe_allow_html=True
)

# -------------------------------------------------------------
# 1. System Overview Page
# -------------------------------------------------------------
if nav_choice == "System Overview":
    st.markdown("<div class='main-header'>Spatio-Temporal Big Data Analytics for Extreme Weather Events</div>", unsafe_allow_html=True)

    # Key Metrics Banner
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown("""
        <div class='metric-box'>
            <div class='metric-label'>Total Weather Events</div>
            <div class='metric-value'>337,951</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown("""
        <div class='metric-box'>
            <div class='metric-label'>Total Fatalities</div>
            <div class='metric-value'>5,980</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown("""
        <div class='metric-box'>
            <div class='metric-label'>Total Injuries</div>
            <div class='metric-value'>10,881</div>
        </div>
        """, unsafe_allow_html=True)
    with col4:
        st.markdown("""
        <div class='metric-box'>
            <div class='metric-label'>Total Economic Damage</div>
            <div class='metric-value'>$104.8 B</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("### Annual Overview & Event Distribution")
    _, yearly_df, event_type_df, _ = load_historical_data()
    row1_c1, row1_c2 = st.columns([1, 1])
    with row1_c1:
        if not yearly_df.empty:
            st.plotly_chart(plot_yearly_trend(yearly_df), use_container_width=True)
    with row1_c2:
        if not event_type_df.empty:
            st.plotly_chart(plot_event_types(event_type_df, top_n=8), use_container_width=True)

# -------------------------------------------------------------
# 2. Historical Analysis Page
# -------------------------------------------------------------
elif nav_choice == "Historical Analysis":
    st.markdown("<div class='main-header'>Historical Extreme Weather Trends</div>", unsafe_allow_html=True)
    st.markdown("<div class='sub-header'>Multi-year longitudinal analysis of extreme meteorological events across the United States.</div>", unsafe_allow_html=True)

    _, yearly_df, event_type_df, yearly_events_df = load_historical_data()

    if not yearly_df.empty:
        c1, c2 = st.columns([1, 1])
        with c1:
            st.plotly_chart(plot_yearly_trend(yearly_df), use_container_width=True)
        with c2:
            st.plotly_chart(plot_event_types(event_type_df, top_n=12), use_container_width=True)

        st.markdown("### Annual Statistical Summary")
        st.dataframe(
            yearly_df.rename(columns={
                "EVENT_YEAR": "Year",
                "total_events": "Total Events",
                "fatalities": "Fatalities",
                "injuries": "Injuries",
                "damage_m_usd": "Damage ($M USD)"
            }),
            use_container_width=True
        )

        st.markdown("### Event Breakdown by Selected Year")
        selected_year = st.selectbox("Select Year to Filter", options=[2020, 2021, 2022, 2023, 2024], index=4)
        if not yearly_events_df.empty:
            df_filtered_year = yearly_events_df[yearly_events_df["EVENT_YEAR"] == selected_year].head(10)
            st.dataframe(
                df_filtered_year.rename(columns={
                    "EVENT_YEAR": "Year",
                    "EVENT_TYPE": "Event Type",
                    "count": "Frequency"
                }),
                use_container_width=True
            )

# -------------------------------------------------------------
# 3. Spatial Analysis Page
# -------------------------------------------------------------
elif nav_choice == "Spatial Analysis":
    st.markdown("<div class='main-header'>Geographic & Spatial Analytics</div>", unsafe_allow_html=True)
    st.markdown("<div class='sub-header'>Spatial density mapping, state-level rankings, and geographic hotspot clusters.</div>", unsafe_allow_html=True)

    _, state_df, state_event_df, hotspots_df = load_spatial_data()

    # Filter Bar
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    with filter_col1:
        metric_choice = st.selectbox(
            "Ranking Metric",
            options=["event_count", "total_casualties", "fatalities", "total_damage_m_usd"],
            format_func=lambda x: {
                "event_count": "Total Event Frequency",
                "total_casualties": "Total Casualties",
                "fatalities": "Fatalities",
                "total_damage_m_usd": "Total Economic Damage ($M)"
            }.get(x, x)
        )
    with filter_col2:
        year_filter = st.selectbox("Map Year Filter", options=["All", "2020", "2021", "2022", "2023", "2024"])
    with filter_col3:
        event_types_list = ["All"] + sorted(hotspots_df["EVENT_TYPE"].unique().tolist()) if not hotspots_df.empty else ["All"]
        event_filter = st.selectbox("Map Event Type Filter", options=event_types_list[:15])

    # Interactive Geospatial Map
    if not hotspots_df.empty:
        st.plotly_chart(plot_us_map(hotspots_df, selected_year=year_filter, selected_event_type=event_filter), use_container_width=True)

    # State Rankings
    if not state_df.empty:
        st.plotly_chart(plot_state_ranking(state_df, metric=metric_choice, top_n=15), use_container_width=True)

        with st.expander("View Complete State Rankings Table"):
            st.dataframe(
                state_df.rename(columns={
                    "STATE": "State",
                    "event_count": "Total Events",
                    "fatalities": "Fatalities",
                    "injuries": "Injuries",
                    "total_casualties": "Total Casualties",
                    "property_damage_m_usd": "Property Damage ($M)",
                    "crop_damage_m_usd": "Crop Damage ($M)",
                    "total_damage_m_usd": "Total Damage ($M)"
                }),
                use_container_width=True
            )

# -------------------------------------------------------------
# 4. Temporal Analysis Page
# -------------------------------------------------------------
elif nav_choice == "Temporal Analysis":
    st.markdown("<div class='main-header'>Temporal & Seasonal Patterns</div>", unsafe_allow_html=True)
    st.markdown("<div class='sub-header'>Analysis of monthly recurrence, seasonal severity, and cyclical event frequency.</div>", unsafe_allow_html=True)

    _, monthly_df, seasonal_df, matrix_df = load_temporal_data()

    if not monthly_df.empty and not seasonal_df.empty:
        col1, col2 = st.columns([1, 1])
        with col1:
            st.plotly_chart(plot_monthly_distribution(monthly_df), use_container_width=True)
        with col2:
            st.plotly_chart(plot_seasonal_distribution(seasonal_df), use_container_width=True)

    if not matrix_df.empty:
        st.markdown("### Year vs. Month Intensity Matrix")
        st.plotly_chart(plot_year_month_heatmap(matrix_df), use_container_width=True)

# -------------------------------------------------------------
# 5. Impact & Severity Analysis Page
# -------------------------------------------------------------
elif nav_choice == "Impact & Severity Analysis":
    st.markdown("<div class='main-header'>Human & Economic Severity Analysis</div>", unsafe_allow_html=True)
    st.markdown("<div class='sub-header'>Direct casualties and financial property vs. crop losses classified by event categories.</div>", unsafe_allow_html=True)

    _, casualty_df, damage_df, tornado_df, top_events_df = load_impact_data()

    if not casualty_df.empty:
        st.plotly_chart(plot_casualty_breakdown(casualty_df, top_n=10), use_container_width=True)

    if not damage_df.empty:
        st.plotly_chart(plot_economic_damage(damage_df, top_n=10), use_container_width=True)

    st.markdown("### Tornado Severity Analysis (Enhanced Fujita Scale)")
    if not tornado_df.empty:
        st.dataframe(
            tornado_df.rename(columns={
                "TOR_F_SCALE": "EF Rating",
                "tornado_count": "Recorded Tornadoes",
                "fatalities": "Fatalities",
                "injuries": "Injuries",
                "damage_m_usd": "Damage ($M USD)"
            }),
            use_container_width=True
        )

    st.markdown("### Top Individual Extreme Weather Events on Record")
    if not top_events_df.empty:
        st.dataframe(
            top_events_df.rename(columns={
                "EVENT_ID": "Event ID",
                "EVENT_TYPE": "Event Type",
                "STATE": "State",
                "COUNTY_ZONE": "County / Zone",
                "EVENT_YEAR": "Year",
                "MONTH_NAME": "Month",
                "DAMAGE_PROPERTY_USD": "Property Damage ($)",
                "DAMAGE_CROPS_USD": "Crop Damage ($)",
                "TOTAL_DAMAGE_USD": "Total Damage ($)",
                "TOTAL_FATALITIES": "Fatalities",
                "TOTAL_INJURIES": "Injuries"
            }),
            use_container_width=True
        )

# -------------------------------------------------------------
# 6. Simulated Real-Time Monitoring Page
# -------------------------------------------------------------
elif nav_choice == "Simulated Real-Time Monitoring":
    st.markdown("<div class='main-header'>Simulated Real-Time Weather-Event Monitoring</div>", unsafe_allow_html=True)
    st.markdown("<div class='sub-header'>Ingestion and stream capture using Apache Flume, buffering into HDFS and live rolling processor.</div>", unsafe_allow_html=True)

    st.warning("""
    **Note on Streaming Mode:** This is a **Simulated Real-Time Stream** replaying genuine historical NOAA weather-event records sequentially.
    It demonstrates live stream ingestion through Apache Flume into HDFS rather than an active live government sensor feed.
    """)

    events_list, stream_metrics = load_streaming_buffer()

    status_col1, status_col2, status_col3 = st.columns(3)
    with status_col1:
        st.markdown(f"""
        <div class='metric-box'>
            <div class='metric-label'>Ingested Stream Events</div>
            <div class='metric-value'>{len(events_list)}</div>
        </div>
        """, unsafe_allow_html=True)
    with status_col2:
        last_time = stream_metrics.get("last_event_time", "No stream active")
        st.markdown(f"""
        <div class='metric-box'>
            <div class='metric-label'>Latest Event Dispatched</div>
            <div class='metric-value' style='font-size: 1.1rem;'>{last_time}</div>
        </div>
        """, unsafe_allow_html=True)
    with status_col3:
        status_text = stream_metrics.get("status", "READY")
        st.markdown(f"""
        <div class='metric-box'>
            <div class='metric-label'>Ingestion Pipeline Status</div>
            <div class='metric-value' style='font-size: 1.1rem; color: #27AE60;'>{status_text}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("### Controls & Replay Trigger")
    ctrl_col1, ctrl_col2 = st.columns([1, 2])
    with ctrl_col1:
        num_replay = st.number_input("Number of events to stream", min_value=5, max_value=200, value=25, step=5)
        if st.button("Trigger Simulated Stream Burst"):
            with st.spinner("Dispatching simulated event stream to Flume / HDFS buffer..."):
                os.system(f"python3 /home/chinmay/bigdata-project/weather-project/streaming/replay_noaa.py {num_replay} 0.05")
                os.system(f"python3 /home/chinmay/bigdata-project/weather-project/streaming/process_stream.py")
            st.session_state["stream_msg"] = f"Streamed {num_replay} events to pipeline."
            st.rerun()

        if "stream_msg" in st.session_state:
            st.success(st.session_state.pop("stream_msg"))

    with ctrl_col2:
        st.markdown("""
        **Pipeline Flow:**
        `replay_noaa.py` $\\rightarrow$ `Flume Netcat Source (Port 44444)` $\\rightarrow$ `Memory Channel` $\\rightarrow$ `HDFS Sink (/weather/streaming/)` $\\rightarrow$ `process_stream.py` $\\rightarrow$ `Dashboard`
        """)

    st.markdown("### Recent Live Event Feed (Rolling Buffer)")
    if events_list:
        feed_df = pd.DataFrame(events_list)
        st.dataframe(
            feed_df.rename(columns={
                "event_id": "Event ID",
                "event_type": "Event Type",
                "state": "State",
                "timestamp": "Event Time",
                "stream_arrival_time": "Stream Arrival",
                "fatalities": "Fatalities",
                "injuries": "Injuries",
                "damage_property": "Property Damage"
            })[["Event ID", "Event Type", "State", "Event Time", "Stream Arrival", "Fatalities", "Injuries", "Property Damage"]],
            use_container_width=True
        )
    else:
        st.info("No events in current buffer. Click 'Trigger Simulated Stream Burst' above to replay events.")

st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #95A5A6; font-size: 0.85rem;'>"
    "Spatio-Temporal Big Data Analytics for Extreme Weather Events using NOAA Data | College Big Data Project<br>"
    "Technologies: Apache Hadoop HDFS 3.5.0 | Apache Hive 4.2.1 | Apache Spark 4.2.0 | Apache Flume 1.11.0 | Streamlit"
    "</div>",
    unsafe_allow_html=True
)
