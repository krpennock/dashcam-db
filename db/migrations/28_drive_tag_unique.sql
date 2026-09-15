BEGIN;

-- One session per (vehicle, drive tag).
--
-- Deliberately separate from migration 27 and applied only after the repair:
-- until then every drive_tag was NULL and 239 sessions still carried an id from
-- the old _02 naming bug, so this index would have had nothing to enforce.
--
-- With it in place a drive cannot quietly end up in the database twice. A
-- re-processed drive works out to the same tag and therefore the same id, so the
-- loader updates the drive it already has.
--
-- NULLs do not conflict in a unique index, so a session whose tag could not be
-- read from its notes does not block this; the repair report lists any such
-- session under "sessions whose name could not be read".
CREATE UNIQUE INDEX IF NOT EXISTS drive_session_vehicle_drive_tag_uidx
  ON dashcam.drive_session (vehicle_tag, drive_tag);

COMMIT;
