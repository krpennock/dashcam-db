-- Auto-generated stage tables (required by ingest_manifest.py)
-- Generated from CSV headers in /srv/dashcam/import/ready/camry/...

CREATE SCHEMA IF NOT EXISTS dashcam;

CREATE UNLOGGED TABLE IF NOT EXISTS dashcam.gnss_csv_stage (
  "ms" bigint,
  "t_rel_s" double precision,
  "utc_time" text,
  "lat" double precision,
  "lon" double precision,
  "speed_knots" double precision,
  "speed_mps" double precision,
  "speed_mph" double precision,
  "course_deg" double precision,
  "rmc_status" text,
  "fix_quality" integer,
  "satellites" integer,
  "hdop" double precision,
  "alt_m" double precision,
  "geoid_sep_m" double precision,
  "date_ddmmyy" text,
  "time_hhmmss" text
);

CREATE UNLOGGED TABLE IF NOT EXISTS dashcam.accel_csv_stage (
  "abs_ms" bigint,
  "t_rel_s" double precision,
  "ax" double precision,
  "ay" double precision,
  "az" double precision,
  "clip" text,
  "idx" integer,
  "t_clip_ms" integer
);
