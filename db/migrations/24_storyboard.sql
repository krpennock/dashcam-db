BEGIN;

CREATE TABLE IF NOT EXISTS dashcam.storyboard_frame (
  drive_session_id uuid NOT NULL
    REFERENCES dashcam.drive_session(drive_session_id)
    ON DELETE CASCADE,
  ts_utc timestamptz NOT NULL,
  local_time timestamptz NULL,
  offset_s double precision NULL,
  source_clip text NULL,
  clip_offset_s double precision NULL,
  file_name text NOT NULL,
  media_rel_path text NOT NULL,
  PRIMARY KEY (drive_session_id, ts_utc)
);

CREATE INDEX IF NOT EXISTS storyboard_frame_session_ts_idx
  ON dashcam.storyboard_frame(drive_session_id, ts_utc);

COMMIT;
