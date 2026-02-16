#!/bin/bash
# Post-start script for GitHub Codespaces

set -e

echo "🚀 Starting Airflow Watcher Demo..."

cd /workspaces/airflow-watcher/demo

# Wait for Docker to be ready
echo "⏳ Waiting for Docker..."
while ! docker info > /dev/null 2>&1; do
    sleep 1
done

# Start Airflow
echo "📦 Starting Airflow containers..."
docker-compose up -d

# Wait for Airflow to be ready
echo "⏳ Waiting for Airflow to be ready..."
for i in {1..60}; do
    if curl -s http://localhost:8080/health | grep -q "healthy"; then
        echo "✅ Airflow is ready!"
        echo ""
        echo "🌐 Open Airflow UI at: http://localhost:8080"
        echo "👤 Login: admin / admin"
        echo ""
        echo "📊 Navigate to 'Watcher' menu to see the monitoring plugin!"
        exit 0
    fi
    sleep 2
done

echo "⚠️ Airflow is still starting up. Check http://localhost:8080 in a moment."
