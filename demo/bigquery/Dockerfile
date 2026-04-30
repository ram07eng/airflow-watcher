FROM python:3.11-slim

WORKDIR /app

COPY bq_client.py app.py ./

RUN pip install --no-cache-dir \
    fastapi==0.115.* \
    uvicorn==0.34.* \
    google-cloud-bigquery==3.41.*

EXPOSE 8080

# Cloud Run injects $PORT; fall back to 8080 for local use
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080}"]
