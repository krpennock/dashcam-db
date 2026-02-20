BEGIN;

CREATE SCHEMA IF NOT EXISTS dashcam;

CREATE TABLE IF NOT EXISTS dashcam.device (
  device_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vendor TEXT NOT NULL DEFAULT 'BlackVue',
  model  TEXT,
  serial TEXT UNIQUE,
  firmware TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS dashcam.drive_session (
  drive_session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  device_id UUID REFERENCES dashcam.device(device_id),
  vehicle_tag TEXT,
  start_ts_utc TIMESTAMPTZ,
  end_ts_utc   TIMESTAMPTZ,
  time_sync JSONB NOT NULL DEFAULT '{}'::jsonb,
  stats JSONB NOT NULL DEFAULT '{}'::jsonb,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS dashcam.clip (
  clip_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  drive_session_id UUID NOT NULL REFERENCES dashcam.drive_session(drive_session_id) ON DELETE CASCADE,
  channel TEXT NOT NULL CHECK (channel IN ('front','rear','interior','other')),
  clip_name TEXT NOT NULL,
  video_path TEXT,
  start_device_ms BIGINT,
  end_device_ms   BIGINT,
  start_ts_utc TIMESTAMPTZ,
  end_ts_utc   TIMESTAMPTZ,
  UNIQUE (drive_session_id, channel, clip_name)
);

CREATE TABLE IF NOT EXISTS dashcam.gnss_sample (
  drive_session_id UUID NOT NULL REFERENCES dashcam.drive_session(drive_session_id) ON DELETE CASCADE,

  device_ms BIGINT NOT NULL,
  t_rel_s DOUBLE PRECISION,
  ts_utc TIMESTAMPTZ,

  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,

  geom GEOGRAPHY(POINT,4326)
    GENERATED ALWAYS AS (
      CASE WHEN lat IS NULL OR lon IS NULL
      THEN NULL
      ELSE ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography END
    ) STORED,

  speed_knots DOUBLE PRECISION,
  speed_mps   DOUBLE PRECISION,
  speed_mph   DOUBLE PRECISION,
  course_deg  DOUBLE PRECISION,

  rmc_status CHAR(1),
  fix_quality SMALLINT,
  satellites  SMALLINT,
  hdop DOUBLE PRECISION,
  alt_m DOUBLE PRECISION,
  geoid_sep_m DOUBLE PRECISION,

  date_ddmmyy CHAR(6),
  time_hhmmss TEXT,

  PRIMARY KEY (drive_session_id, device_ms)
);

CREATE INDEX IF NOT EXISTS gnss_ts_idx   ON dashcam.gnss_sample (ts_utc);
CREATE INDEX IF NOT EXISTS gnss_geom_idx ON dashcam.gnss_sample USING GIST (geom);

CREATE INDEX IF NOT EXISTS gnss_valid_ts_idx
ON dashcam.gnss_sample (ts_utc)
WHERE rmc_status='A' AND fix_quality > 0 AND lat IS NOT NULL AND lon IS NOT NULL;

CREATE TABLE IF NOT EXISTS dashcam.accel_sample (
  drive_session_id UUID NOT NULL REFERENCES dashcam.drive_session(drive_session_id) ON DELETE CASCADE,

  abs_ms BIGINT NOT NULL,
  t_rel_s DOUBLE PRECISION,
  ts_utc TIMESTAMPTZ,

  ax_raw SMALLINT NOT NULL,
  ay_raw SMALLINT NOT NULL,
  az_raw SMALLINT NOT NULL,

  clip_name TEXT NOT NULL,
  idx INT NOT NULL,
  t_clip_ms INT,

  clip_id UUID REFERENCES dashcam.clip(clip_id),

  ax_g DOUBLE PRECISION,
  ay_g DOUBLE PRECISION,
  az_g DOUBLE PRECISION,
  a_mag_g DOUBLE PRECISION,

  source_meta JSONB NOT NULL DEFAULT '{}'::jsonb,

  PRIMARY KEY (drive_session_id, clip_name, idx)
);

CREATE INDEX IF NOT EXISTS accel_ts_idx ON dashcam.accel_sample (ts_utc);
CREATE INDEX IF NOT EXISTS accel_abs_ms_idx ON dashcam.accel_sample (abs_ms);
CREATE INDEX IF NOT EXISTS accel_clip_t_idx ON dashcam.accel_sample (clip_id, t_clip_ms);

CREATE TABLE IF NOT EXISTS dashcam.sensor_calibration (
  calibration_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  device_id UUID NOT NULL REFERENCES dashcam.device(device_id) ON DELETE CASCADE,
  channel TEXT CHECK (channel IN ('front','rear','interior','other')),
  created_ts_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
  model JSONB NOT NULL,
  notes TEXT
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'event_type' AND typnamespace = 'dashcam'::regnamespace) THEN
    CREATE TYPE dashcam.event_type AS ENUM
      ('impact','hard_brake','hard_accel','sharp_turn','lane_change','speeding','other');
  END IF;
END$$;

CREATE TABLE IF NOT EXISTS dashcam.event_marker (
  drive_session_id UUID NOT NULL REFERENCES dashcam.drive_session(drive_session_id) ON DELETE CASCADE,
  ts_utc TIMESTAMPTZ NOT NULL,
  type dashcam.event_type NOT NULL,
  severity DOUBLE PRECISION,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (drive_session_id, ts_utc, type)
);

COMMIT;
