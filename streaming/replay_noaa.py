"""
Module: replay_noaa.py
Description: Simulated Real-Time NOAA Weather Event Streamer.
Replays genuine NOAA historical events sequentially with a configurable delay.
Streams events via TCP socket to Apache Flume (localhost:44444) and maintains a rolling
live event buffer for the Streamlit dashboard.
"""
import sys
import os
import time
import json
import socket
import glob
import gzip
import csv

PROJECT_ROOT = "/home/chinmay/bigdata-project/weather-project"
BUFFER_FILE = os.path.join(PROJECT_ROOT, "data", "streaming", "live_stream_buffer.json")
STREAM_METRICS_FILE = os.path.join(PROJECT_ROOT, "output", "stream_metrics.json")
FLUME_HOST = "127.0.1.1"
FLUME_PORT = 44444


def stream_events(max_events=100, delay=0.5):
    os.makedirs(os.path.dirname(BUFFER_FILE), exist_ok=True)
    os.makedirs(os.path.dirname(STREAM_METRICS_FILE), exist_ok=True)

    # Locate a NOAA source file
    source_files = sorted(glob.glob(os.path.join(PROJECT_ROOT, "data", "raw", "*.csv.gz")), reverse=True)
    if not source_files:
        print("[Streamer Error] No raw NOAA CSV.GZ files found in data/raw!")
        return

    sample_file = source_files[0]
    print(f"[Simulated Streamer] Reading historical records from {os.path.basename(sample_file)}...")

    # Attempt to open socket to Flume Netcat source
    flume_sock = None
    try:
        flume_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        flume_sock.settimeout(2.0)
        flume_sock.connect((FLUME_HOST, FLUME_PORT))
        print(f"[Simulated Streamer] Connected to Apache Flume Agent at {FLUME_HOST}:{FLUME_PORT}")
    except Exception as e:
        print(f"[Simulated Streamer Notice] Flume agent not running on {FLUME_HOST}:{FLUME_PORT} ({e}). Events will be recorded to live stream buffer.")
        flume_sock = None

    rolling_events = []
    if os.path.exists(BUFFER_FILE):
        try:
            with open(BUFFER_FILE, "r") as f:
                rolling_events = json.load(f)
        except Exception:
            rolling_events = []

    sent_count = 0
    with gzip.open(sample_file, "rt", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if sent_count >= max_events:
                break

            event_id = row.get("EVENT_ID", "")
            event_type = row.get("EVENT_TYPE", "Severe Weather")
            state = row.get("STATE", "UNKNOWN")
            date_time = row.get("BEGIN_DATE_TIME", "")
            fatalities = int(row.get("DEATHS_DIRECT", 0) or 0) + int(row.get("DEATHS_INDIRECT", 0) or 0)
            injuries = int(row.get("INJURIES_DIRECT", 0) or 0) + int(row.get("INJURIES_INDIRECT", 0) or 0)
            lat = float(row.get("BEGIN_LAT", 0.0) or 0.0)
            lon = float(row.get("BEGIN_LON", 0.0) or 0.0)
            damage_str = row.get("DAMAGE_PROPERTY", "0.00K")

            event_payload = {
                "event_id": event_id,
                "event_type": event_type,
                "state": state,
                "timestamp": date_time,
                "stream_arrival_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "fatalities": fatalities,
                "injuries": injuries,
                "latitude": lat,
                "longitude": lon,
                "damage_property": damage_str
            }

            line = json.dumps(event_payload) + "\n"

            # 1. Send to Flume via TCP if connected
            if flume_sock:
                try:
                    flume_sock.sendall(line.encode("utf-8"))
                except Exception as e:
                    print(f"[Streamer Warning] Failed sending to Flume socket: {e}")
                    flume_sock = None

            # 2. Add to rolling buffer (keep latest 50 events)
            rolling_events.insert(0, event_payload)
            rolling_events = rolling_events[:50]

            with open(BUFFER_FILE, "w") as bf:
                json.dump(rolling_events, bf, indent=2)

            # 3. Update streaming metrics
            metrics = {
                "total_streamed": sent_count + 1,
                "last_event_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "STREAMING ACTIVE",
                "flume_connected": flume_sock is not None
            }
            with open(STREAM_METRICS_FILE, "w") as mf:
                json.dump(metrics, mf, indent=2)

            sent_count += 1
            print(f"[{sent_count}/{max_events}] Dispatched: {event_type} in {state} (Injuries: {injuries}, Fatalities: {fatalities})")
            time.sleep(delay)

    if flume_sock:
        flume_sock.close()

    print(f"[Simulated Streamer] Dispatched {sent_count} events successfully.")


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    delay_sec = float(sys.argv[2]) if len(sys.argv) > 2 else 0.2
    stream_events(max_events=count, delay=delay_sec)
