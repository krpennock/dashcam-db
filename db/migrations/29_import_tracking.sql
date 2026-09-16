BEGIN;

-- What arrived from a camera card, and what became of each drive in it.
--
-- Until now a drive that failed to load left nothing behind but a folder moved
-- into failed/ and whatever scrolled past in a terminal: five January drives sat
-- unimported for months with no record of why. These two tables are what the
-- Imports page reads, and what makes a failure visible instead of silent.

CREATE TABLE IF NOT EXISTS dashcam.import_batch (
  batch_id        uuid PRIMARY KEY,
  source_kind     text NOT NULL
                    CHECK (source_kind IN ('sdcard', 'folder', 'wifi', 'legacy')),
  source_key      text,                     -- card serial, folder path, camera address
  vehicle_tag     text,
  camera_serial   text,
  camera_model    text,
  camera_firmware text,

  clips_on_source integer,                  -- what the source held
  clips_new       integer,                  -- what was actually new to us
  bytes_new       bigint,

  -- The PC's side of the story: when the card appeared, when copying finished,
  -- when processing finished. The server fills in the rest.
  detected_at     timestamptz,
  acquired_at     timestamptz,
  processed_at    timestamptz,
  received_at     timestamptz NOT NULL DEFAULT now(),
  completed_at    timestamptz,

  status          text NOT NULL DEFAULT 'received'
                    CHECK (status IN ('received', 'ingesting', 'retry_wait',
                                      'completed', 'completed_with_errors', 'failed')),
  attempts        integer NOT NULL DEFAULT 0,
  error           text,
  pc_summary      jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS import_batch_received_idx
  ON dashcam.import_batch (received_at DESC);
CREATE INDEX IF NOT EXISTS import_batch_status_idx
  ON dashcam.import_batch (status);
CREATE INDEX IF NOT EXISTS import_batch_vehicle_idx
  ON dashcam.import_batch (vehicle_tag, received_at DESC);

CREATE TABLE IF NOT EXISTS dashcam.import_item (
  item_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  batch_id         uuid NOT NULL
                     REFERENCES dashcam.import_batch(batch_id) ON DELETE CASCADE,
  vehicle_tag      text NOT NULL,
  drive_tag        text NOT NULL,
  view             text NOT NULL DEFAULT 'front',

  -- Deliberately no foreign key: an item records what was attempted, including
  -- drives that were never loaded and drives whose session was later deleted.
  drive_session_id uuid,

  clip_pairs       integer,
  first_clip       text,
  last_clip        text,

  status           text NOT NULL
                     CHECK (status IN ('ingested', 'skipped_short', 'parking_only',
                                       'quarantined', 'failed_processing', 'failed',
                                       'needs_review', 'superseded')),
  reason_code      text,                    -- e.g. NO_ACCEL_CSV, short machine-readable
  reason           text,                    -- the same thing in words, for the page
  counts           jsonb NOT NULL DEFAULT '{}'::jsonb,
  time_confidence  text,

  created_at       timestamptz NOT NULL DEFAULT now(),
  started_at       timestamptz,
  finished_at      timestamptz,

  UNIQUE (batch_id, vehicle_tag, drive_tag, view)
);

CREATE INDEX IF NOT EXISTS import_item_batch_idx
  ON dashcam.import_item (batch_id);
CREATE INDEX IF NOT EXISTS import_item_status_idx
  ON dashcam.import_item (status, created_at DESC);
CREATE INDEX IF NOT EXISTS import_item_drive_idx
  ON dashcam.import_item (vehicle_tag, drive_tag);
CREATE INDEX IF NOT EXISTS import_item_session_idx
  ON dashcam.import_item (drive_session_id);

-- Where a session came from, and whether it is protected from the holding-window
-- clean-up on the PC. source_batch_id has no foreign key on purpose: a batch
-- record may be cleaned up long before the drives it delivered.
ALTER TABLE dashcam.drive_session
  ADD COLUMN IF NOT EXISTS camera_serial   text,
  ADD COLUMN IF NOT EXISTS source_batch_id uuid,
  ADD COLUMN IF NOT EXISTS ingested_at     timestamptz,
  ADD COLUMN IF NOT EXISTS keep            boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS drive_session_source_batch_idx
  ON dashcam.drive_session (source_batch_id);

COMMENT ON COLUMN dashcam.drive_session.camera_serial IS
  'Serial read out of the clips themselves, so a drive can be traced to a camera.';
COMMENT ON COLUMN dashcam.drive_session.source_batch_id IS
  'The import_batch this session arrived in, where it is known.';
COMMENT ON COLUMN dashcam.drive_session.keep IS
  'Set by hand to stop the PC deleting this drive''s video from the holding window.';

COMMIT;
