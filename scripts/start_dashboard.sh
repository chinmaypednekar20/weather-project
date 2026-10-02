#!/bin/bash
# Script to launch the Streamlit Web Dashboard
source /home/chinmay/bigdata-project/env.sh

PORT=${1:-8501}

echo "=========================================================="
echo " Starting Extreme Weather Analytics Dashboard...          "
echo " Access URL: http://localhost:$PORT                       "
echo "=========================================================="

streamlit run /home/chinmay/bigdata-project/weather-project/dashboard/app.py \
    --server.port "$PORT" \
    --server.headless true
