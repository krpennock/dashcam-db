FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest_manifest.py .
# The importer service and the one-off repair tool run from the same image, so
# they always match the loader they call.
COPY importer/ ./importer/

# Loading one drive folder stays the default, which is what the older
# scripts/dashcam_ingest_ready.sh expects. The importer service overrides this
# with: python -m importer.run
ENTRYPOINT ["python", "/app/ingest_manifest.py"]
