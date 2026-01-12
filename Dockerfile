FROM apache/airflow:2.7.0-python3.9

USER root

# Install system dependencies if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy the plugin
COPY --chown=airflow:root . /opt/airflow/plugins/airflow-watcher/

# Install the plugin
RUN pip install --no-cache-dir -e /opt/airflow/plugins/airflow-watcher/

# Set environment variables for the plugin
ENV AIRFLOW__AIRFLOW_WATCHER__SLACK_CHANNEL="#airflow-alerts"
ENV AIRFLOW__AIRFLOW_WATCHER__FAILURE_LOOKBACK_HOURS=24
ENV AIRFLOW__AIRFLOW_WATCHER__SLA_CHECK_INTERVAL_MINUTES=5
