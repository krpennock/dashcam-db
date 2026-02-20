# Dashcam DB/App state (as of 2026-02-19)


Repo root: ~/dashcam-db on Debian

Stack: PostGIS container dashcam-postgis, FastAPI dashcam-api, nginx dashcam-ui, tools profile ingest

Ingest: sudo docker compose --profile tools run --rm ingest /import/<drive>/manifest.json

Manifest v2 fields used: drive_session_id, vehicle_tag, telemetry.gnss.path, telemetry.accel.path, (nmea optional), clips[], session_time.start_ts_utc/end_ts_utc

Tables/views in play:

dashcam.drive_session, dashcam.clip, dashcam.gnss_sample, dashcam.accel_sample

dashcam.storyboard_frame (populated from artifacts/thumbs/index.csv + jpgs copied into ./media)

API endpoints: /sessions, /sessions/{id}, /sessions/{id}/track, /sessions/{id}/gnss, /sessions/{id}/accel, /sessions/{id}/storyboard, /media/...

Known fixes applied:

accel ts_utc derived from drive_session.start_ts_utc + t_rel_s

storyboard ingest copies thumbs from /import/.../artifacts/thumbs/ into /media/<vehicle>/<sid>/thumbs/

UI: accel chart “double load” fixed by forcing x-axis range after plot

Current goal: polish UI (storyboard viewer, formatting cleanups), keep manifest/drives script contract compliant.

