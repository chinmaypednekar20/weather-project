"""
Charts module for Extreme Weather Analytics System.
Generates clean, academic Plotly visualizations with restrained palettes.
"""
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Academic Color Palette
COLOR_PRIMARY = "#2C3E50"    # Deep Navy Slate
COLOR_SECONDARY = "#3498DB"  # Calibrated Blue
COLOR_ACCENT = "#E67E22"     # Amber / Orange
COLOR_MUTED = "#95A5A6"      # Neutral Grey
COLOR_DANGER = "#C0392B"     # Brick Red
COLOR_FOREST = "#27AE60"     # Forest Green


def plot_yearly_trend(yearly_df):
    """Line chart showing total extreme weather events per year."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=yearly_df["EVENT_YEAR"],
        y=yearly_df["total_events"],
        mode="lines+markers",
        line=dict(color=COLOR_PRIMARY, width=3),
        marker=dict(size=8, color=COLOR_SECONDARY),
        name="Annual Events"
    ))
    fig.update_layout(
        title="Annual Extreme Weather Event Frequency (2020 - 2024)",
        xaxis_title="Year",
        yaxis_title="Total Recorded Events",
        template="simple_white",
        margin=dict(l=40, r=40, t=50, b=40),
        height=380
    )
    return fig


def plot_event_types(event_type_df, top_n=12):
    """Horizontal bar chart for top event types."""
    df_sorted = event_type_df.head(top_n).sort_values("event_count", ascending=True)
    fig = go.Figure(go.Bar(
        x=df_sorted["event_count"],
        y=df_sorted["EVENT_TYPE"],
        orientation="h",
        marker_color=COLOR_PRIMARY
    ))
    fig.update_layout(
        title=f"Top {top_n} Severe Weather Event Categories",
        xaxis_title="Event Count",
        yaxis_title="Event Category",
        template="simple_white",
        margin=dict(l=150, r=30, t=50, b=40),
        height=420
    )
    return fig


def plot_us_map(hotspots_df, selected_year=None, selected_event_type="All"):
    """Scattergeo map of US extreme weather events."""
    df_filtered = hotspots_df.copy()
    if selected_year and selected_year != "All":
        df_filtered = df_filtered[df_filtered["EVENT_YEAR"] == int(selected_year)]
    if selected_event_type and selected_event_type != "All":
        df_filtered = df_filtered[df_filtered["EVENT_TYPE"] == selected_event_type]

    # Limit display points for fast interactive rendering
    if len(df_filtered) > 4000:
        df_filtered = df_filtered.sample(4000, random_state=42)

    fig = px.scatter_geo(
        df_filtered,
        lat="LATITUDE",
        lon="LONGITUDE",
        color="EVENT_TYPE",
        hover_name="EVENT_TYPE",
        hover_data={"STATE": True, "TOTAL_FATALITIES": True, "TOTAL_INJURIES": True, "LATITUDE": False, "LONGITUDE": False},
        scope="usa",
        title="Geospatial Distribution of Severe Weather Events",
        opacity=0.6
    )
    fig.update_traces(marker=dict(size=4))
    fig.update_layout(
        geo=dict(
            lakecolor="rgb(240, 244, 248)",
            landcolor="rgb(248, 249, 250)",
            subunitcolor="rgb(200, 205, 210)",
            countrycolor="rgb(180, 185, 190)"
        ),
        margin=dict(l=10, r=10, t=40, b=10),
        height=480,
        legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5)
    )
    return fig


def plot_state_ranking(state_df, metric="event_count", top_n=15):
    """Horizontal bar chart for state rankings."""
    metric_labels = {
        "event_count": "Total Events",
        "total_casualties": "Total Casualties (Fatalities + Injuries)",
        "fatalities": "Total Fatalities",
        "total_damage_m_usd": "Total Economic Damage ($M USD)"
    }
    df_sorted = state_df.sort_values(metric, ascending=True).tail(top_n)

    fig = go.Figure(go.Bar(
        x=df_sorted[metric],
        y=df_sorted["STATE"],
        orientation="h",
        marker_color=COLOR_SECONDARY
    ))
    fig.update_layout(
        title=f"Top {top_n} States by {metric_labels.get(metric, metric)}",
        xaxis_title=metric_labels.get(metric, metric),
        yaxis_title="State",
        template="simple_white",
        margin=dict(l=120, r=30, t=50, b=40),
        height=420
    )
    return fig


def plot_monthly_distribution(monthly_df):
    """Bar chart showing monthly event frequency across all years."""
    month_order = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    df_sorted = monthly_df.sort_values("EVENT_MONTH")
    fig = go.Figure(go.Bar(
        x=df_sorted["MONTH_NAME"],
        y=df_sorted["event_count"],
        marker_color=COLOR_PRIMARY
    ))
    fig.update_layout(
        title="Monthly Extreme Weather Event Distribution (5-Year Aggregate)",
        xaxis=dict(categoryorder="array", categoryarray=month_order, title="Month"),
        yaxis_title="Total Events",
        template="simple_white",
        margin=dict(l=40, r=40, t=50, b=40),
        height=380
    )
    return fig


def plot_seasonal_distribution(seasonal_df):
    """Bar chart for seasonal patterns."""
    fig = go.Figure(go.Bar(
        x=seasonal_df["SEASON"],
        y=seasonal_df["event_count"],
        marker_color=[COLOR_SECONDARY, COLOR_FOREST, COLOR_ACCENT, COLOR_PRIMARY]
    ))
    fig.update_layout(
        title="Seasonal Weather Event Frequency",
        xaxis_title="Season",
        yaxis_title="Event Count",
        template="simple_white",
        margin=dict(l=40, r=40, t=50, b=40),
        height=350
    )
    return fig


def plot_year_month_heatmap(matrix_df):
    """Heatmap matrix of month versus year."""
    pivot_df = matrix_df.pivot(index="MONTH_NAME", columns="EVENT_YEAR", values="event_count")
    month_order = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    # Reindex months in calendar order
    existing_months = [m for m in month_order if m in pivot_df.index]
    pivot_df = pivot_df.reindex(existing_months)

    fig = go.Figure(go.Heatmap(
        z=pivot_df.values,
        x=[str(c) for c in pivot_df.columns],
        y=pivot_df.index,
        colorscale="Blues",
        colorbar=dict(title="Events")
    ))
    fig.update_layout(
        title="Year vs. Month Event Density Heatmap",
        xaxis_title="Year",
        yaxis_title="Month",
        margin=dict(l=80, r=40, t=50, b=40),
        height=400
    )
    return fig


def plot_casualty_breakdown(casualty_df, top_n=10):
    """Grouped bar chart for fatalities vs injuries."""
    df_top = casualty_df.head(top_n)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_top["EVENT_TYPE"],
        y=df_top["fatalities"],
        name="Fatalities",
        marker_color=COLOR_DANGER
    ))
    fig.add_trace(go.Bar(
        x=df_top["EVENT_TYPE"],
        y=df_top["injuries"],
        name="Injuries",
        marker_color=COLOR_ACCENT
    ))
    fig.update_layout(
        title=f"Top {top_n} Hazardous Event Types by Human Casualties",
        barmode="group",
        xaxis_title="Event Type",
        yaxis_title="Casualty Count",
        template="simple_white",
        margin=dict(l=40, r=40, t=50, b=80),
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig


def plot_economic_damage(damage_df, top_n=10):
    """Stacked bar chart for Property vs Crop Damage."""
    df_top = damage_df.head(top_n)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_top["EVENT_TYPE"],
        y=df_top["property_damage_m_usd"],
        name="Property Damage ($M)",
        marker_color=COLOR_PRIMARY
    ))
    fig.add_trace(go.Bar(
        x=df_top["EVENT_TYPE"],
        y=df_top["crop_damage_m_usd"],
        name="Crop Damage ($M)",
        marker_color=COLOR_FOREST
    ))
    fig.update_layout(
        title=f"Top {top_n} Event Types by Financial Impact ($M USD)",
        barmode="stack",
        xaxis_title="Event Type",
        yaxis_title="Damage in Million USD",
        template="simple_white",
        margin=dict(l=40, r=40, t=50, b=80),
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig
