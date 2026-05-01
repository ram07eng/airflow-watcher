#!/usr/bin/env python
"""Launcher for the standalone Airflow Watcher API.

Usage:
    python run_api.py

Starts the FastAPI server on the port configured in .env (default 8083).
Does NOT require apache-airflow to be installed — uses lightweight stubs.
"""

import os
import sys

# Add src/ to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

# Install Airflow stubs if Airflow is not installed
from airflow_watcher.api.compat import install_airflow_stubs, reflect_airflow_models

install_airflow_stubs()

import uvicorn  # noqa: E402

from airflow_watcher.api.db import get_engine  # noqa: E402
from airflow_watcher.api.main import create_app  # noqa: E402

app, cfg = create_app()

# Now that DB is initialised, reflect real Airflow tables onto the stub models
engine = get_engine()
if engine is not None:
    reflect_airflow_models(engine)

uvicorn.run(app, host=cfg.api_host, port=cfg.api_port)
