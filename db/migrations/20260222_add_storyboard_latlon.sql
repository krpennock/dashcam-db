ALTER TABLE dashcam.storyboard_frame
  ADD COLUMN IF NOT EXISTS lat double precision,
  ADD COLUMN IF NOT EXISTS lon double precision,
  ADD COLUMN IF NOT EXISTS speed_mph double precision,
  ADD COLUMN IF NOT EXISTS course_deg double precision;

CREATE INDEX IF NOT EXISTS storyboard_frame_sid_ts
  ON dashcam.storyboard_frame (drive_session_id, ts_utc);
