FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest_manifest.py .

ENTRYPOINT ["python", "/app/ingest_manifest.py"]
