#!/bin/bash
# Script to launch Apache Flume agent for simulated real-time ingestion
source /home/chinmay/bigdata-project/env.sh

echo "=============================================="
echo " Starting Apache Flume Ingestion Agent...     "
echo " Configuration: flume/weather_flume.conf      "
echo " Listening on: 127.0.1.1:44444 (Netcat Source)"
echo " Sink destination: hdfs://localhost:9000/weather/streaming/"
echo "=============================================="

flume-ng agent \
    --conf /home/chinmay/bigdata-project/apache-flume-1.11.0/conf \
    --conf-file /home/chinmay/bigdata-project/weather-project/flume/weather_flume.conf \
    --name weather_agent \
    -Dflume.root.logger=INFO,console
