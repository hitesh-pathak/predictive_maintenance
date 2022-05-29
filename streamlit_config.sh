#! /bin/bash
mkdir -p ~/.streamlit/

PORT=${PORT:-8501}

echo -e "\
[server]\n\
headless = true\n\
enableCORS=false\n\
port = $PORT\n\
" > ~/.streamlit/config.toml

echo "streamlit config done!"