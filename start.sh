#!/usr/bin/env bash
set -e
pip install -r requirements.txt
uvicorn backend_api:app --host 0.0.0.0 --port 10000