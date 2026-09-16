"""Watch the import tree and load whatever turns up.

The PC ships a batch of processed drives into ``<import root>/ready/`` and this
service picks it up: it loads each drive, files the folder into ``done/`` or
``failed/``, records what happened to every drive in the database, and sends one
notification per batch.

Run it as a service (the compose ``importer`` service), or once:

    python -m importer.run --once

Two shapes are understood under ``ready/``:

* ``ready/<batch uuid>/`` with a ``batch.json`` describing the batch and the
  drives in it, which is what the PC pipeline sends.
* ``ready/<vehicle>/<drive folder>/`` — the older hand-run layout. Those are
  loaded too and recorded as a ``legacy`` batch, so nothing that worked before
  stops working.

Nothing here deletes anything: folders are moved, and a collision renames the
older folder aside rather than overwriting it.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import signal
import sys
import time
import traceback
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import psycopg

# The image copies ingest_manifest.py next to this package.
sys.path.insert(0, "/app")
try:
    from ingest_manifest import IngestDataError, ingest_drive
except ImportError:  # running from a checkout
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from ingest_manifest import IngestDataError, ingest_drive


LOCK_NAME = ".importer.lock"
BATCH_FILE = "batch.json"

#: How long to wait before trying a batch again, by attempt number. A database
#: that is briefly down should not turn an import into a failure.
RETRY_BACKOFF_MINUTES = [1, 5, 30, 120]

#: Statuses a batch item can arrive with. Only "processed" is something to load;
#: the rest are outcomes the PC already decided and we simply record.
LOADABLE = "processed"

UUID_RE_LEN = 36


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {msg}", flush=True)


def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise SystemExit(f"Missing env var: {name}")
    return v


def is_uuid(text: str) -> bool:
    if len(text) != UUID_RE_LEN:
        return False
    try:
        uuid.UUID(text)
        return True
    except ValueError:
        return False


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

class Config:
    def __init__(self) -> None:
        self.import_root = Path(os.getenv("IMPORT_ROOT", "/import"))
        self.media_root = Path(os.getenv("MEDIA_ROOT", "/media"))
        self.poll_seconds = float(os.getenv("IMPORTER_POLL_SECONDS", "15"))
        self.min_free_gb = float(os.getenv("IMPORTER_MIN_FREE_GB", "10"))
        self.ntfy_url = (os.getenv("NTFY_URL") or "").rstrip("/")
        self.ntfy_topic = os.getenv("NTFY_TOPIC") or ""
        self.ntfy_token = os.getenv("NTFY_TOKEN") or ""
        self.viewer_url = (os.getenv("VIEWER_URL") or "").rstrip("/")

    def dsn(self) -> dict[str, Any]:
        return {
            "host": env("DB_HOST", "db"),
            "port": env("DB_PORT", "5432"),
            "dbname": env("DB_NAME", "dashcam"),
            "user": env("DB_USER", "dashcam"),
            "password": env("DB_PASSWORD", "change_me_now"),
        }

    @property
    def ready(self) -> Path:
        return self.import_root / "ready"

    @property
    def processing(self) -> Path:
        return self.import_root / "processing"

    @property
    def done(self) -> Path:
        return self.import_root / "done"

    @property
    def failed(self) -> Path:
        return self.import_root / "failed"


# --------------------------------------------------------------------------
# Notifications. A missing or broken notifier must never stop an import.
# --------------------------------------------------------------------------

def notify(cfg: Config, title: str, message: str, *, tags: str = "", priority: str = "") -> None:
    if not cfg.ntfy_url or not cfg.ntfy_topic:
        return
    req = urllib.request.Request(
        f"{cfg.ntfy_url}/{cfg.ntfy_topic}",
        data=message.encode("utf-8"),
        method="POST",
    )
    req.add_header("Title", title)
    if tags:
        req.add_header("Tags", tags)
    if priority:
        req.add_header("Priority", priority)
    if cfg.ntfy_token:
        req.add_header("Authorization", f"Bearer {cfg.ntfy_token}")
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
    except (urllib.error.URLError, OSError) as exc:
        log(f"notification failed (carrying on): {exc}")


# --------------------------------------------------------------------------
# Moving folders about
# --------------------------------------------------------------------------

def move_aside(dest: Path) -> None:
    """Rename an existing folder out of the way instead of overwriting it."""
    if dest.exists():
        stamp = utcnow().strftime("%Y%m%d%H%M%S")
        dest.rename(dest.with_name(f"{dest.name}.prev_{stamp}"))


def file_drive(folder: Path, root: Path, vehicle: str) -> Path:
    """Move a drive folder under done/ or failed/, keeping any older copy."""
    target_dir = root / vehicle
    target_dir.mkdir(parents=True, exist_ok=True)
    dest = target_dir / folder.name
    move_aside(dest)
    shutil.move(str(folder), str(dest))
    return dest


def free_gb(path: Path) -> float:
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except OSError:
        return float("inf")


# --------------------------------------------------------------------------
# Database records
# --------------------------------------------------------------------------

def upsert_batch(conn: psycopg.Connection, batch: dict[str, Any]) -> None:
    camera = batch.get("camera") or {}
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dashcam.import_batch (
              batch_id, source_kind, source_key, vehicle_tag,
              camera_serial, camera_model, camera_firmware,
              clips_on_source, clips_new, bytes_new,
              detected_at, acquired_at, processed_at, received_at,
              status, pc_summary
            )
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, now(), 'received', %s::jsonb)
            ON CONFLICT (batch_id) DO UPDATE SET
              source_kind = EXCLUDED.source_kind,
              source_key = COALESCE(EXCLUDED.source_key, dashcam.import_batch.source_key),
              vehicle_tag = COALESCE(EXCLUDED.vehicle_tag, dashcam.import_batch.vehicle_tag),
              camera_serial = COALESCE(EXCLUDED.camera_serial, dashcam.import_batch.camera_serial),
              camera_model = COALESCE(EXCLUDED.camera_model, dashcam.import_batch.camera_model),
              camera_firmware = COALESCE(EXCLUDED.camera_firmware, dashcam.import_batch.camera_firmware),
              clips_on_source = COALESCE(EXCLUDED.clips_on_source, dashcam.import_batch.clips_on_source),
              clips_new = COALESCE(EXCLUDED.clips_new, dashcam.import_batch.clips_new),
              bytes_new = COALESCE(EXCLUDED.bytes_new, dashcam.import_batch.bytes_new),
              pc_summary = EXCLUDED.pc_summary;
            """,
            (
                batch["batch_id"], batch.get("source_kind") or "folder", batch.get("source_key"),
                batch.get("vehicle_tag"), camera.get("serial"), camera.get("model"),
                camera.get("firmware"), batch.get("clips_on_source"), batch.get("clips_new"),
                batch.get("bytes_new"), batch.get("detected_at"), batch.get("acquired_at"),
                batch.get("processed_at"), json.dumps(batch.get("pc_summary") or {}),
            ),
        )


def set_batch_status(
    conn: psycopg.Connection,
    batch_id: str,
    status: str,
    *,
    error: Optional[str] = None,
    completed: bool = False,
    bump_attempts: bool = False,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE dashcam.import_batch
               SET status = %s,
                   error = %s,
                   attempts = attempts + %s,
                   completed_at = {'now()' if completed else 'completed_at'}
             WHERE batch_id = %s::uuid;
            """,
            (status, error, 1 if bump_attempts else 0, batch_id),
        )


def record_item(
    conn: psycopg.Connection,
    batch_id: str,
    item: dict[str, Any],
    *,
    status: str,
    reason_code: Optional[str] = None,
    reason: Optional[str] = None,
    counts: Optional[dict[str, Any]] = None,
    drive_session_id: Optional[str] = None,
    time_confidence: Optional[str] = None,
    finished: bool = True,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dashcam.import_item (
              batch_id, vehicle_tag, drive_tag, view, drive_session_id,
              clip_pairs, first_clip, last_clip, status, reason_code, reason,
              counts, time_confidence, started_at, finished_at
            )
            VALUES (%s::uuid, %s, %s, %s, %s::uuid, %s, %s, %s, %s, %s, %s,
                    %s::jsonb, %s, now(), %s)
            ON CONFLICT (batch_id, vehicle_tag, drive_tag, view) DO UPDATE SET
              drive_session_id = EXCLUDED.drive_session_id,
              clip_pairs = COALESCE(EXCLUDED.clip_pairs, dashcam.import_item.clip_pairs),
              first_clip = COALESCE(EXCLUDED.first_clip, dashcam.import_item.first_clip),
              last_clip = COALESCE(EXCLUDED.last_clip, dashcam.import_item.last_clip),
              status = EXCLUDED.status,
              reason_code = EXCLUDED.reason_code,
              reason = EXCLUDED.reason,
              counts = EXCLUDED.counts,
              time_confidence = EXCLUDED.time_confidence,
              finished_at = EXCLUDED.finished_at;
            """,
            (
                batch_id, item.get("vehicle_tag"), item.get("drive_tag"),
                item.get("view") or "front", drive_session_id,
                item.get("clip_pairs"), item.get("first_clip"), item.get("last_clip"),
                status, reason_code, reason, json.dumps(counts or {}),
                time_confidence, utcnow() if finished else None,
            ),
        )


def link_session_to_batch(conn: psycopg.Connection, sid: str, batch_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE dashcam.drive_session SET source_batch_id = %s::uuid "
            "WHERE drive_session_id = %s::uuid;",
            (batch_id, sid),
        )


def batch_attempts(conn: psycopg.Connection, batch_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT attempts FROM dashcam.import_batch WHERE batch_id = %s::uuid;", (batch_id,))
        row = cur.fetchone()
        return int(row[0]) if row else 0


# --------------------------------------------------------------------------
# Loading one drive
# --------------------------------------------------------------------------

class TransientError(Exception):
    """The database (not the data) is the problem, so the batch waits and retries."""


def load_one_drive(
    conn: psycopg.Connection,
    cfg: Config,
    batch_id: str,
    item: dict[str, Any],
    folder: Path,
) -> dict[str, Any]:
    """Load a drive folder and record the outcome. Returns the item result."""
    manifest = folder / "manifest.json"
    if not manifest.exists():
        record_item(conn, batch_id, item, status="failed",
                    reason_code="NO_MANIFEST",
                    reason="the drive folder has no manifest, so there is nothing to load")
        return {"status": "failed", "folder": folder, "ok": False}

    try:
        result = ingest_drive(conn, manifest, cfg.media_root, batch_id=batch_id)
    except IngestDataError as exc:
        record_item(conn, batch_id, item, status="failed",
                    reason_code=exc.code, reason=exc.message)
        log(f"  {folder.name}: failed ({exc.code})")
        return {"status": "failed", "folder": folder, "ok": False}
    except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
        raise TransientError(str(exc)) from exc
    except psycopg.errors.DataError as exc:
        # Something in the drive's own telemetry cannot be stored. That is a fact
        # about the data, not a passing problem, so it is recorded in plain words
        # rather than leaving a Python exception name on the Imports page.
        record_item(conn, batch_id, item, status="failed",
                    reason_code="BAD_TELEMETRY_VALUE",
                    reason=f"a value in the telemetry files could not be stored: {exc}"[:500])
        log(f"  {folder.name}: failed (a telemetry value could not be stored)")
        return {"status": "failed", "folder": folder, "ok": False}
    except Exception as exc:  # unexpected, but one drive must not stop the batch
        record_item(conn, batch_id, item, status="failed",
                    reason_code="UNEXPECTED",
                    reason=f"{type(exc).__name__}: {exc}"[:500])
        log(f"  {folder.name}: unexpected failure: {exc}\n{traceback.format_exc()}")
        return {"status": "failed", "folder": folder, "ok": False}

    link_session_to_batch(conn, result["drive_session_id"], batch_id)
    status = "needs_review" if result.get("needs_review") else "ingested"
    reason = "; ".join(result.get("warnings") or []) or None
    record_item(
        conn, batch_id, item, status=status,
        reason_code="CLOCK_MISMATCH" if result.get("needs_review") else None,
        reason=reason,
        counts=result.get("counts"),
        drive_session_id=result["drive_session_id"],
        time_confidence=result.get("time_confidence"),
    )
    log(f"  {folder.name}: {status} ({result['counts']})")
    return {"status": status, "folder": folder, "ok": True, "result": result}


# --------------------------------------------------------------------------
# Batches
# --------------------------------------------------------------------------

def read_batch_file(batch_dir: Path) -> Optional[dict[str, Any]]:
    path = batch_dir / BATCH_FILE
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        log(f"{batch_dir.name}: batch.json is not valid JSON ({exc})")
        return None
    data.setdefault("batch_id", batch_dir.name)
    return data


def process_batch(conn: psycopg.Connection, cfg: Config, batch_dir: Path) -> dict[str, Any]:
    """Load every drive in one shipped batch and file its folders away."""
    batch = read_batch_file(batch_dir)
    if batch is None:
        # Deliveries only become visible once the receiving script has verified
        # them, so one without a readable batch.json is broken. Move it out of
        # ready/ instead of finding it again on every pass for ever.
        stash = cfg.failed / "_batches" / batch_dir.name
        stash.parent.mkdir(parents=True, exist_ok=True)
        move_aside(stash)
        shutil.move(str(batch_dir), str(stash))
        log(f"{batch_dir.name}: no usable batch.json; moved to {stash}")
        return {"skipped": True}

    batch_id = str(batch["batch_id"])
    upsert_batch(conn, batch)
    set_batch_status(conn, batch_id, "ingesting", bump_attempts=True)

    items = batch.get("items") or []
    loaded = failed = recorded = 0

    for item in items:
        rel = item.get("folder")
        status = (item.get("status") or LOADABLE).strip()

        if status != LOADABLE:
            # The PC already decided: too short, parking only, and so on.
            record_item(conn, batch_id, item, status=status,
                        reason_code=item.get("reason_code"), reason=item.get("reason"),
                        counts=item.get("counts"))
            recorded += 1
            continue

        # The folder path comes out of the delivery's own batch.json, so it is
        # checked rather than trusted: a drive folder has to sit inside the
        # batch folder, which rules out a path that climbs out of it.
        folder = None
        if rel:
            candidate = (batch_dir / rel).resolve()
            if candidate.is_dir() and batch_dir.resolve() in candidate.parents:
                folder = candidate

        if folder is None:
            record_item(conn, batch_id, item, status="failed",
                        reason_code="FOLDER_MISSING",
                        reason=f"the batch lists {rel!r}, which is not a folder inside the delivery")
            failed += 1
            continue

        outcome = load_one_drive(conn, cfg, batch_id, item, folder)
        vehicle = item.get("vehicle_tag") or folder.parent.name
        file_drive(folder, cfg.done if outcome["ok"] else cfg.failed, vehicle)
        loaded += 1 if outcome["ok"] else 0
        failed += 0 if outcome["ok"] else 1

    # Keep the delivery note with the drives it describes.
    batches_dir = cfg.done / "_batches"
    batches_dir.mkdir(parents=True, exist_ok=True)
    src = batch_dir / BATCH_FILE
    if src.exists():
        dest = batches_dir / f"{batch_id}.json"
        move_aside(dest)
        shutil.move(str(src), str(dest))

    # The checksums were verified by the receiving script when the delivery
    # arrived, so they have served their purpose.
    (batch_dir / "SHA256SUMS").unlink(missing_ok=True)

    leftovers = [p for p in batch_dir.rglob("*") if p.is_file()]
    if leftovers:
        # Something arrived that the batch did not describe. Keep it for a look,
        # but out of ready/, or every later pass would pick it up again.
        stash = cfg.failed / "_batches" / batch_id
        stash.parent.mkdir(parents=True, exist_ok=True)
        move_aside(stash)
        shutil.move(str(batch_dir), str(stash))
        log(f"{batch_id}: {len(leftovers)} file(s) were not described by the batch; kept in {stash}")
    else:
        shutil.rmtree(batch_dir, ignore_errors=True)

    status = "completed" if failed == 0 else ("failed" if loaded == 0 else "completed_with_errors")
    set_batch_status(conn, batch_id, status, completed=True)
    return {"batch_id": batch_id, "loaded": loaded, "failed": failed,
            "recorded": recorded, "status": status,
            "vehicle_tag": batch.get("vehicle_tag")}


def process_legacy(conn: psycopg.Connection, cfg: Config, vehicle_dir: Path) -> Optional[dict[str, Any]]:
    """Load drive folders left in the old ready/<vehicle>/<drive>/ layout."""
    drives = sorted(p for p in vehicle_dir.iterdir() if p.is_dir() and (p / "manifest.json").exists())
    if not drives:
        return None

    vehicle = vehicle_dir.name
    batch_id = str(uuid.uuid4())
    upsert_batch(conn, {
        "batch_id": batch_id,
        "source_kind": "legacy",
        "source_key": str(vehicle_dir),
        "vehicle_tag": vehicle,
        "clips_new": None,
        "pc_summary": {"note": "drives found in the older ready/<vehicle>/ layout"},
    })
    set_batch_status(conn, batch_id, "ingesting", bump_attempts=True)

    loaded = failed = 0
    for folder in drives:
        # A folder is named <tag>_<view>; fall back to that when the manifest
        # does not state the tag itself.
        name, view = folder.name, "front"
        for candidate in ("front", "rear", "interior", "other"):
            if name.endswith(f"_{candidate}"):
                name, view = name[: -(len(candidate) + 1)], candidate
                break
        item = {"vehicle_tag": vehicle, "drive_tag": name, "view": view}

        try:
            man = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
            item["drive_tag"] = man.get("drive_tag") or item["drive_tag"]
            item["vehicle_tag"] = man.get("vehicle_tag") or vehicle
        except (OSError, ValueError):
            pass

        outcome = load_one_drive(conn, cfg, batch_id, item, folder)
        file_drive(folder, cfg.done if outcome["ok"] else cfg.failed, item["vehicle_tag"])
        loaded += 1 if outcome["ok"] else 0
        failed += 0 if outcome["ok"] else 1

    status = "completed" if failed == 0 else ("failed" if loaded == 0 else "completed_with_errors")
    set_batch_status(conn, batch_id, status, completed=True)
    return {"batch_id": batch_id, "loaded": loaded, "failed": failed,
            "recorded": 0, "status": status, "vehicle_tag": vehicle}


def recover_processing(cfg: Config) -> int:
    """Put anything left in processing/ back into ready/ after a crash."""
    if not cfg.processing.is_dir():
        return 0
    moved = 0
    for item in sorted(cfg.processing.iterdir()):
        dest = cfg.ready / item.name
        move_aside(dest)
        shutil.move(str(item), str(dest))
        moved += 1
    if moved:
        log(f"recovered {moved} item(s) from processing/ after an interrupted run")
    return moved


# --------------------------------------------------------------------------
# One pass
# --------------------------------------------------------------------------

def summarise(outcome: dict[str, Any]) -> str:
    bits = [f"{outcome['loaded']} loaded"]
    if outcome.get("failed"):
        bits.append(f"{outcome['failed']} failed")
    if outcome.get("recorded"):
        bits.append(f"{outcome['recorded']} not sent")
    return ", ".join(bits)


def run_once(cfg: Config) -> int:
    """Process everything currently in ready/. Returns how many batches ran."""
    cfg.ready.mkdir(parents=True, exist_ok=True)
    recover_processing(cfg)

    entries = sorted(p for p in cfg.ready.iterdir() if p.is_dir())
    if not entries:
        return 0

    free = free_gb(cfg.import_root)
    if free < cfg.min_free_gb:
        notify(cfg, "Dashcam: disk almost full",
               f"Only {free:.1f} GB free on the server. Imports will keep running, "
               f"but clear some space soon.", tags="warning", priority="high")

    done = 0
    with psycopg.connect(**cfg.dsn(), autocommit=True) as conn:
        for entry in entries:
            try:
                if is_uuid(entry.name):
                    outcome = process_batch(conn, cfg, entry)
                else:
                    outcome = process_legacy(conn, cfg, entry)
                if not outcome or outcome.get("skipped"):
                    continue
            except TransientError as exc:
                # The database went away mid-batch: leave everything where it is
                # and try again later rather than filing drives as failures.
                attempts = 0
                try:
                    attempts = batch_attempts(conn, str(entry.name)) if is_uuid(entry.name) else 0
                    wait = RETRY_BACKOFF_MINUTES[min(attempts, len(RETRY_BACKOFF_MINUTES) - 1)]
                    status = "retry_wait" if attempts < len(RETRY_BACKOFF_MINUTES) else "failed"
                    set_batch_status(conn, str(entry.name), status, error=str(exc)[:500])
                    log(f"{entry.name}: database unavailable ({exc}); {status}, next try in ~{wait} min")
                except Exception:
                    log(f"{entry.name}: database unavailable ({exc}); will retry")
                raise

            done += 1
            vehicle = outcome.get("vehicle_tag") or "dashcam"
            title = f"Dashcam: {vehicle} import {outcome['status'].replace('_', ' ')}"
            body = summarise(outcome)
            if cfg.viewer_url:
                body += f"\n{cfg.viewer_url}/imports.html?batch={outcome['batch_id']}"
            notify(cfg, title, body,
                   tags="white_check_mark" if outcome["status"] == "completed" else "warning",
                   priority="default" if outcome["status"] == "completed" else "high")
            log(f"{outcome['batch_id']}: {outcome['status']} ({body.splitlines()[0]})")

    return done


def main(argv: Optional[Iterable[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Load drives that arrive in the import tree.")
    ap.add_argument("--once", action="store_true", help="Process what is waiting, then stop.")
    ap.add_argument("--poll-seconds", type=float, default=None, help="Override the poll interval.")
    args = ap.parse_args(list(argv) if argv is not None else None)

    cfg = Config()
    if args.poll_seconds:
        cfg.poll_seconds = args.poll_seconds

    cfg.import_root.mkdir(parents=True, exist_ok=True)
    lock_path = cfg.import_root / LOCK_NAME
    lock = open(lock_path, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log("another importer already holds the lock; stopping")
        return 0

    stopping = False

    def _stop(_sig, _frame):
        nonlocal stopping
        stopping = True
        log("stop requested; finishing the current pass")

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    log(f"importer watching {cfg.ready} (every {cfg.poll_seconds:.0f}s)")
    while True:
        try:
            run_once(cfg)
        except TransientError:
            pass  # already logged and recorded; wait and try again
        except Exception as exc:
            log(f"pass failed: {exc}\n{traceback.format_exc()}")
        if args.once or stopping:
            return 0
        time.sleep(cfg.poll_seconds)


if __name__ == "__main__":
    sys.exit(main())
