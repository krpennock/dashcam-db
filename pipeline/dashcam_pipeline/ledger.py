"""What the pipeline remembers between runs.

A small SQLite file (``state.sqlite`` under the state folder) recording every
clip it has seen, every drive it has built, every delivery it has sent, and every
notification it still owes. It is what makes the pipeline safe to interrupt:

* a clip already in here is never copied off a card again, so re-inserting a card
  is cheap and produces nothing;
* a drive already built is never rebuilt, and a drive already loaded is never
  sent twice;
* the holding-window clean-up can tell which clips still belong to a drive that
  has not reached the database, and leave those alone.

Every timestamp is ISO-8601 UTC text, because SQLite has no date type and this
sorts correctly as a string.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

SCHEMA_VERSION = 1

#: A clip's life: copied off the card, moved to the holding folder, deleted by
#: the holding window, or found to disagree with a clip we already had.
CLIP_STATES = ("staged", "held", "purged", "conflict")

#: A drive's life. The last four are outcomes, not failures to retry.
DRIVE_STATES = (
    "planned",        # clips grouped, nothing built yet
    "processed",      # the drive folder exists and is complete
    "shipped",        # handed to the server, not yet confirmed
    "ingested",       # the server says it is in the database
    "skipped_short",  # too few clips to be a drive
    "parking_only",   # no driving clips; video held, nothing loaded
    "needs_review",   # the clips do not fit the drives we know about
    "failed",         # processing or delivery gave up
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clip (
  vehicle_tag TEXT NOT NULL,
  stem        TEXT NOT NULL,          -- 20260806_094436_NF
  ts_key      TEXT NOT NULL,          -- 20260806_094436
  channel     TEXT NOT NULL,          -- front | rear
  clip_type   TEXT,                   -- N E P I M
  size_bytes  INTEGER NOT NULL,
  head_sha1   TEXT,                   -- first megabyte
  tail_sha1   TEXT,                   -- last megabyte
  source_kind TEXT,
  source_key  TEXT,
  acquired_at TEXT,
  state       TEXT NOT NULL,
  staged_path TEXT,
  held_path   TEXT,
  PRIMARY KEY (vehicle_tag, stem)
);

CREATE INDEX IF NOT EXISTS clip_by_time  ON clip (vehicle_tag, ts_key);
CREATE INDEX IF NOT EXISTS clip_by_state ON clip (state, acquired_at);

CREATE TABLE IF NOT EXISTS drive (
  vehicle_tag      TEXT NOT NULL,
  drive_tag        TEXT NOT NULL,
  view             TEXT NOT NULL DEFAULT 'front',
  drive_session_id TEXT,
  clip_set_sha1    TEXT,
  clip_pairs       INTEGER,
  first_clip       TEXT,
  last_clip        TEXT,
  status           TEXT NOT NULL,
  reason_code      TEXT,
  reason           TEXT,
  folder           TEXT,
  batch_id         TEXT,
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  PRIMARY KEY (vehicle_tag, drive_tag, view)
);

CREATE INDEX IF NOT EXISTS drive_by_status ON drive (status, updated_at);
CREATE INDEX IF NOT EXISTS drive_by_batch  ON drive (batch_id);

CREATE TABLE IF NOT EXISTS drive_clip (
  vehicle_tag TEXT NOT NULL,
  drive_tag   TEXT NOT NULL,
  stem        TEXT NOT NULL,
  PRIMARY KEY (vehicle_tag, drive_tag, stem)
);

CREATE INDEX IF NOT EXISTS drive_clip_by_stem ON drive_clip (vehicle_tag, stem);

CREATE TABLE IF NOT EXISTS batch (
  batch_id      TEXT PRIMARY KEY,
  vehicle_tag   TEXT,
  source_kind   TEXT,
  source_key    TEXT,
  drives        INTEGER,
  clips_new     INTEGER,
  bytes_new     INTEGER,
  created_at    TEXT NOT NULL,
  shipped_at    TEXT,
  confirmed_at  TEXT,
  server_status TEXT,
  attempts      INTEGER NOT NULL DEFAULT 0,
  last_error    TEXT
);

-- Notifications are written here first and sent afterwards, so nothing is lost
-- when the network (or the phone) is unavailable.
CREATE TABLE IF NOT EXISTS outbox (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  kind       TEXT NOT NULL,
  title      TEXT NOT NULL,
  message    TEXT NOT NULL,
  tags       TEXT,
  priority   TEXT,
  sent_at    TEXT,
  attempts   INTEGER NOT NULL DEFAULT 0,
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (sent_at, created_at);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Ledger:
    """The pipeline's memory. One instance per process; SQLite does the locking."""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        # WAL lets the watcher write while a status command reads.
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.executescript(SCHEMA)
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES ('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
            (str(SCHEMA_VERSION),),
        )

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Group related writes so an interruption cannot leave half of them."""
        self.conn.execute("BEGIN IMMEDIATE;")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK;")
            raise
        self.conn.execute("COMMIT;")

    # --- clips -----------------------------------------------------------

    def known_stems(self, vehicle_tag: str) -> set[str]:
        """Every clip this vehicle has ever given us, whatever became of it."""
        rows = self.conn.execute(
            "SELECT stem FROM clip WHERE vehicle_tag = ?;", (vehicle_tag,)
        ).fetchall()
        return {r["stem"] for r in rows}

    def get_clip(self, vehicle_tag: str, stem: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM clip WHERE vehicle_tag = ? AND stem = ?;", (vehicle_tag, stem)
        ).fetchone()

    def record_clip(self, **fields: Any) -> None:
        """Add or update one clip. Unknown state values are refused."""
        state = fields.get("state")
        if state not in CLIP_STATES:
            raise ValueError(f"unknown clip state {state!r}")
        fields.setdefault("acquired_at", utcnow())
        columns = (
            "vehicle_tag", "stem", "ts_key", "channel", "clip_type", "size_bytes",
            "head_sha1", "tail_sha1", "source_kind", "source_key", "acquired_at",
            "state", "staged_path", "held_path",
        )
        values = [fields.get(c) for c in columns]
        updates = ", ".join(
            f"{c}=excluded.{c}" for c in columns if c not in ("vehicle_tag", "stem", "acquired_at")
        )
        self.conn.execute(
            f"INSERT INTO clip ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))}) "
            f"ON CONFLICT(vehicle_tag, stem) DO UPDATE SET {updates};",
            values,
        )

    def set_clip_state(self, vehicle_tag: str, stem: str, state: str, **fields: Any) -> None:
        if state not in CLIP_STATES:
            raise ValueError(f"unknown clip state {state!r}")
        sets = ["state = ?"]
        values: list[Any] = [state]
        for key in ("staged_path", "held_path"):
            if key in fields:
                sets.append(f"{key} = ?")
                values.append(fields[key])
        values += [vehicle_tag, stem]
        self.conn.execute(
            f"UPDATE clip SET {', '.join(sets)} WHERE vehicle_tag = ? AND stem = ?;", values
        )

    def clips_in_state(self, state: str, vehicle_tag: Optional[str] = None) -> list[sqlite3.Row]:
        if vehicle_tag:
            return self.conn.execute(
                "SELECT * FROM clip WHERE state = ? AND vehicle_tag = ? ORDER BY ts_key;",
                (state, vehicle_tag),
            ).fetchall()
        return self.conn.execute(
            "SELECT * FROM clip WHERE state = ? ORDER BY vehicle_tag, ts_key;", (state,)
        ).fetchall()

    def clips_near(self, vehicle_tag: str, ts_key_from: str, ts_key_to: str) -> list[sqlite3.Row]:
        """Clips whose timestamp falls in a window, for regrouping a drive."""
        return self.conn.execute(
            "SELECT * FROM clip WHERE vehicle_tag = ? AND ts_key >= ? AND ts_key <= ? "
            "ORDER BY ts_key;",
            (vehicle_tag, ts_key_from, ts_key_to),
        ).fetchall()

    # --- drives ----------------------------------------------------------

    def record_drive(self, vehicle_tag: str, drive_tag: str, *, view: str = "front", **fields: Any) -> None:
        status = fields.get("status")
        if status not in DRIVE_STATES:
            raise ValueError(f"unknown drive status {status!r}")
        now = utcnow()
        columns = (
            "vehicle_tag", "drive_tag", "view", "drive_session_id", "clip_set_sha1",
            "clip_pairs", "first_clip", "last_clip", "status", "reason_code", "reason",
            "folder", "batch_id", "created_at", "updated_at",
        )
        payload = dict(fields)
        payload.update(vehicle_tag=vehicle_tag, drive_tag=drive_tag, view=view,
                       created_at=now, updated_at=now)
        values = [payload.get(c) for c in columns]
        updates = ", ".join(
            f"{c}=excluded.{c}" for c in columns
            if c not in ("vehicle_tag", "drive_tag", "view", "created_at")
        )
        self.conn.execute(
            f"INSERT INTO drive ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))}) "
            f"ON CONFLICT(vehicle_tag, drive_tag, view) DO UPDATE SET {updates};",
            values,
        )

    def set_drive_status(self, vehicle_tag: str, drive_tag: str, status: str,
                         *, view: str = "front", **fields: Any) -> None:
        if status not in DRIVE_STATES:
            raise ValueError(f"unknown drive status {status!r}")
        sets = ["status = ?", "updated_at = ?"]
        values: list[Any] = [status, utcnow()]
        for key in ("drive_session_id", "clip_set_sha1", "clip_pairs", "reason_code",
                    "reason", "folder", "batch_id"):
            if key in fields:
                sets.append(f"{key} = ?")
                values.append(fields[key])
        values += [vehicle_tag, drive_tag, view]
        self.conn.execute(
            f"UPDATE drive SET {', '.join(sets)} "
            f"WHERE vehicle_tag = ? AND drive_tag = ? AND view = ?;",
            values,
        )

    def get_drive(self, vehicle_tag: str, drive_tag: str, view: str = "front") -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM drive WHERE vehicle_tag = ? AND drive_tag = ? AND view = ?;",
            (vehicle_tag, drive_tag, view),
        ).fetchone()

    def drives_with_status(self, *statuses: str) -> list[sqlite3.Row]:
        marks = ", ".join("?" * len(statuses))
        return self.conn.execute(
            f"SELECT * FROM drive WHERE status IN ({marks}) ORDER BY vehicle_tag, drive_tag;",
            statuses,
        ).fetchall()

    def set_drive_clips(self, vehicle_tag: str, drive_tag: str, stems: Iterable[str]) -> None:
        """Replace the clip list of a drive, so a regrouped drive stays accurate."""
        self.conn.execute(
            "DELETE FROM drive_clip WHERE vehicle_tag = ? AND drive_tag = ?;",
            (vehicle_tag, drive_tag),
        )
        self.conn.executemany(
            "INSERT OR IGNORE INTO drive_clip (vehicle_tag, drive_tag, stem) VALUES (?, ?, ?);",
            [(vehicle_tag, drive_tag, s) for s in stems],
        )

    def drive_stems(self, vehicle_tag: str, drive_tag: str) -> list[str]:
        rows = self.conn.execute(
            "SELECT stem FROM drive_clip WHERE vehicle_tag = ? AND drive_tag = ? ORDER BY stem;",
            (vehicle_tag, drive_tag),
        ).fetchall()
        return [r["stem"] for r in rows]

    def drives_for_clip(self, vehicle_tag: str, stem: str) -> list[sqlite3.Row]:
        """Which drives claim this clip. Used before deleting any video."""
        return self.conn.execute(
            "SELECT d.* FROM drive d JOIN drive_clip c "
            "  ON c.vehicle_tag = d.vehicle_tag AND c.drive_tag = d.drive_tag "
            "WHERE c.vehicle_tag = ? AND c.stem = ?;",
            (vehicle_tag, stem),
        ).fetchall()

    def clip_is_safe_to_delete(self, vehicle_tag: str, stem: str) -> bool:
        """True only when this clip is claimed by drives that are all settled.

        A clip belonging to a drive that has not been loaded is never deleted by
        the holding window, however old it is.

        A clip claimed by *no* drive is also kept. That looks over-cautious --
        such a clip was acquired and never planned -- but the alternative is
        worse: if the drive records were ever lost or damaged, every held clip
        would suddenly look unclaimed and the holding window would delete the
        video. Keeping them costs disk; the other way costs footage. `prune`
        reports them separately so they stay visible.
        """
        drives = self.drives_for_clip(vehicle_tag, stem)
        if not drives:
            return False
        settled = {"ingested", "skipped_short", "parking_only", "failed"}
        return all(d["status"] in settled for d in drives)

    # --- batches ---------------------------------------------------------

    def record_batch(self, batch_id: str, **fields: Any) -> None:
        columns = ("batch_id", "vehicle_tag", "source_kind", "source_key", "drives",
                   "clips_new", "bytes_new", "created_at", "shipped_at", "confirmed_at",
                   "server_status", "attempts", "last_error")
        payload = dict(fields)
        payload.setdefault("created_at", utcnow())
        payload.setdefault("attempts", 0)
        payload["batch_id"] = batch_id
        values = [payload.get(c) for c in columns]
        updates = ", ".join(f"{c}=excluded.{c}" for c in columns if c not in ("batch_id", "created_at"))
        self.conn.execute(
            f"INSERT INTO batch ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))}) "
            f"ON CONFLICT(batch_id) DO UPDATE SET {updates};",
            values,
        )

    def update_batch(self, batch_id: str, **fields: Any) -> None:
        if not fields:
            return
        sets = ", ".join(f"{k} = ?" for k in fields)
        self.conn.execute(
            f"UPDATE batch SET {sets} WHERE batch_id = ?;", [*fields.values(), batch_id]
        )

    def bump_batch_attempt(self, batch_id: str, error: Optional[str]) -> int:
        self.conn.execute(
            "UPDATE batch SET attempts = attempts + 1, last_error = ? WHERE batch_id = ?;",
            (error, batch_id),
        )
        row = self.conn.execute(
            "SELECT attempts FROM batch WHERE batch_id = ?;", (batch_id,)
        ).fetchone()
        return int(row["attempts"]) if row else 0

    def unconfirmed_batches(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM batch WHERE confirmed_at IS NULL ORDER BY created_at;"
        ).fetchall()

    def supersede_unshipped_batches(
        self, vehicle_tag: str, keep_batch_id: Optional[str] = None
    ) -> int:
        """Throw away parcels that were made up but never sent.

        When the server cannot be reached, the next run makes a fresh parcel --
        rightly, because by then it may hold more drives. What it must not do is
        leave the old one behind: nothing points at it, it can never be
        confirmed, and it would sit in `status` for ever. A fortnight of the
        server being away would otherwise leave a fortnight of dead parcels.

        Drives pointing at a discarded parcel have their parcel cleared, so the
        next one picks them up again. Without that a drive that is recorded but
        not sent -- one too short to be a drive, say -- would never travel at
        all, and would be missing from the Imports page for good.

        Parcels that *were* sent are left alone however long they stay
        unconfirmed, because the server may still be working through them.
        """
        if keep_batch_id is None:
            rows = self.conn.execute(
                "SELECT batch_id FROM batch WHERE vehicle_tag = ? AND shipped_at IS NULL;",
                (vehicle_tag,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT batch_id FROM batch "
                "WHERE vehicle_tag = ? AND shipped_at IS NULL AND batch_id <> ?;",
                (vehicle_tag, keep_batch_id),
            ).fetchall()
        stale = [r["batch_id"] for r in rows]
        if not stale:
            return 0

        marks = ", ".join("?" * len(stale))
        self.conn.execute(
            f"UPDATE drive SET batch_id = NULL WHERE vehicle_tag = ? AND batch_id IN ({marks});",
            [vehicle_tag, *stale],
        )
        self.conn.execute(f"DELETE FROM batch WHERE batch_id IN ({marks});", stale)
        return len(stale)

    # --- notifications ---------------------------------------------------

    def queue_message(self, kind: str, title: str, message: str,
                      *, tags: str = "", priority: str = "") -> int:
        cur = self.conn.execute(
            "INSERT INTO outbox (created_at, kind, title, message, tags, priority) "
            "VALUES (?, ?, ?, ?, ?, ?);",
            (utcnow(), kind, title, message, tags, priority),
        )
        return int(cur.lastrowid)

    def pending_messages(self, limit: int = 20) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM outbox WHERE sent_at IS NULL ORDER BY created_at LIMIT ?;", (limit,)
        ).fetchall()

    def mark_message_sent(self, message_id: int) -> None:
        self.conn.execute("UPDATE outbox SET sent_at = ? WHERE id = ?;", (utcnow(), message_id))

    def mark_message_failed(self, message_id: int, error: str) -> None:
        self.conn.execute(
            "UPDATE outbox SET attempts = attempts + 1, last_error = ? WHERE id = ?;",
            (error[:500], message_id),
        )

    # --- summaries -------------------------------------------------------

    def counts(self) -> dict[str, dict[str, int]]:
        """A count of clips by state and drives by status, for `status`."""
        clips = {
            r["state"]: r["n"]
            for r in self.conn.execute("SELECT state, count(*) AS n FROM clip GROUP BY state;")
        }
        drives = {
            r["status"]: r["n"]
            for r in self.conn.execute("SELECT status, count(*) AS n FROM drive GROUP BY status;")
        }
        return {"clips": clips, "drives": drives}
