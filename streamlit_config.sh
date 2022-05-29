#! /bin/bash
mkdir -p ~/.streamlit/

PORT=${PORT:-8501}

echo "[server]
headless = true
port = $PORT
enableCORS = false
" > ~/.streamlit/config.toml

echo "streamlit config done!"