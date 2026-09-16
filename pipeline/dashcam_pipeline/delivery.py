"""Handing finished drives to the server, and finding out what became of them.

A delivery is a tar holding:

    batch.json                       what this is, and which drives are in it
    SHA256SUMS                       every file below, checksummed
    <vehicle>/<drive folder>/...     the drives themselves

It is streamed straight into ``ssh ... receive <batch id>``, where the server's
gate script unpacks it into a staging folder, checks the sums, and only then lets
the importer see it. A delivery that arrives half-written is therefore never
loaded, and re-sending one that already arrived is answered with
``already_received`` rather than loading it twice.

The raw clips under ``artifacts/clips`` are left out: the server has no use for
them and they are the bulk of a drive folder.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import tarfile
import tempfile
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

#: Folder names inside a drive that are never sent.
SKIP_DIRS = {"clips"}

#: Statuses the server reports that mean it has finished with a delivery.
FINAL_STATUSES = {"completed", "completed_with_errors", "failed"}


class DeliveryError(Exception):
    """The delivery could not be handed over. Worth retrying later."""


@dataclass
class Delivery:
    batch_id: str
    tar_path: Path
    batch: dict[str, Any]
    bytes_total: int


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sendable_files(drive: Path) -> list[Path]:
    """Everything in a drive folder except the raw clip artifacts."""
    out = []
    for p in sorted(drive.rglob("*")):
        if p.is_dir():
            continue
        if any(part in SKIP_DIRS for part in p.relative_to(drive).parts):
            continue
        out.append(p)
    return out


def describe_drive(drive: Path, *, vehicle_tag: str, status: str = "processed",
                   extra: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """The batch.json entry for one drive folder, read from its manifest."""
    manifest_path = drive / "manifest.json"
    manifest: dict[str, Any] = {}
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except ValueError:
            manifest = {}

    clips = [c for c in (manifest.get("clips") or []) if isinstance(c, dict)]
    fronts = sorted(str(c.get("clip_name")) for c in clips if str(c.get("channel")) == "front")

    name, view = drive.name, "front"
    for candidate in ("front", "rear", "interior", "other"):
        if name.endswith(f"_{candidate}"):
            name, view = name[: -(len(candidate) + 1)], candidate
            break

    telemetry = manifest.get("telemetry") or {}
    item = {
        "vehicle_tag": manifest.get("vehicle_tag") or vehicle_tag,
        "drive_tag": manifest.get("drive_tag") or name,
        "view": view,
        "folder": f"{manifest.get('vehicle_tag') or vehicle_tag}/{drive.name}",
        "status": status,
        "clip_pairs": len(fronts),
        "first_clip": fronts[0] if fronts else None,
        "last_clip": fronts[-1] if fronts else None,
        "time_confidence": (manifest.get("session_time") or {}).get("time_confidence"),
        "counts": {
            "clips": len(clips),
            "gnss_rows": (telemetry.get("gnss") or {}).get("rows"),
            "accel_rows": (telemetry.get("accel") or {}).get("rows"),
        },
    }
    if extra:
        item.update(extra)
    return item


def build(
    drives: Sequence[Path],
    *,
    vehicle_tag: str,
    source_kind: str = "sdcard",
    source_key: Optional[str] = None,
    batch_id: Optional[str] = None,
    recorded_items: Sequence[dict[str, Any]] = (),
    pc_summary: Optional[dict[str, Any]] = None,
    workdir: Optional[Path] = None,
) -> Delivery:
    """Lay a delivery out and tar it.

    ``recorded_items`` are drives the PC decided not to send -- too short, parked,
    or failed to process. They travel with the delivery so the Imports page can
    show what happened to everything on the card, not only what loaded.
    """
    batch_id = batch_id or str(uuid.uuid4())
    tmp = Path(workdir) if workdir else Path(tempfile.mkdtemp(prefix="dashcam-batch-"))
    staging = tmp / batch_id
    staging.mkdir(parents=True, exist_ok=True)

    items: list[dict[str, Any]] = []
    checksums: list[tuple[str, str]] = []
    total = 0
    camera: dict[str, Any] = {}

    for drive in drives:
        manifest_path = drive / "manifest.json"
        if manifest_path.is_file():
            try:
                source = (json.loads(manifest_path.read_text(encoding="utf-8")).get("source") or {})
                camera = camera or {
                    "serial": source.get("serial"),
                    "model": source.get("model"),
                    "firmware": source.get("firmware"),
                }
            except ValueError:
                pass

        item = describe_drive(drive, vehicle_tag=vehicle_tag)
        vehicle = item["vehicle_tag"]
        dest_root = staging / vehicle / drive.name

        for src in sendable_files(drive):
            rel = src.relative_to(drive)
            dest = dest_root / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(src.read_bytes())
            total += dest.stat().st_size
            checksums.append((sha256_of(dest), f"{vehicle}/{drive.name}/{rel.as_posix()}"))

        items.append(item)

    items.extend(dict(i) for i in recorded_items)

    batch = {
        "batch_id": batch_id,
        "source_kind": source_kind,
        "source_key": source_key,
        "vehicle_tag": vehicle_tag,
        "camera": camera,
        "clips_new": sum(int(i.get("clip_pairs") or 0) for i in items),
        "clips_on_source": (pc_summary or {}).get("clips_on_source"),
        "bytes_new": total,
        "detected_at": (pc_summary or {}).get("detected_at"),
        "acquired_at": (pc_summary or {}).get("acquired_at"),
        "processed_at": utcnow(),
        "items": items,
        "pc_summary": dict(pc_summary or {}, sent_by="dashcam_pipeline", sent_utc=utcnow()),
    }

    (staging / "batch.json").write_text(json.dumps(batch, indent=2), encoding="utf-8")
    checksums.append((sha256_of(staging / "batch.json"), "batch.json"))
    (staging / "SHA256SUMS").write_text(
        "".join(f"{d}  {n}\n" for d, n in sorted(checksums, key=lambda t: t[1])),
        encoding="utf-8",
    )

    tar_path = tmp / f"{batch_id}.tar"
    with tarfile.open(tar_path, "w") as tar:
        for path in sorted(staging.rglob("*")):
            if path.is_file():
                tar.add(path, arcname=path.relative_to(staging).as_posix())

    return Delivery(batch_id=batch_id, tar_path=tar_path, batch=batch, bytes_total=total)


# --------------------------------------------------------------------------
# Talking to the server
# --------------------------------------------------------------------------

def _ssh_command(host: str, identity: Optional[Path], verb: str) -> list[str]:
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
           "-o", "ConnectTimeout=20"]
    if identity:
        cmd += ["-i", str(identity)]
    cmd += [host, verb]
    return cmd


def ping(host: str, identity: Optional[Path] = None, timeout: int = 30) -> bool:
    """Is the server reachable, and does our key still work?"""
    try:
        done = subprocess.run(_ssh_command(host, identity, "ping"),
                              capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        return False
    return done.returncode == 0 and done.stdout.strip().startswith("pong")


def send(delivery: Delivery, *, host: str, identity: Optional[Path] = None,
         timeout: int = 3600) -> str:
    """Stream a delivery to the server. Returns what the server said."""
    cmd = _ssh_command(host, identity, f"receive {delivery.batch_id}")
    try:
        with delivery.tar_path.open("rb") as f:
            done = subprocess.run(cmd, stdin=f, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise DeliveryError(f"could not reach the server: {exc}") from exc

    reply = (done.stdout or "").strip()
    if done.returncode != 0:
        raise DeliveryError((done.stderr or reply or "the server refused the delivery").strip())
    return reply


def server_status(host: str, batch_id: str, identity: Optional[Path] = None) -> str:
    """waiting | loading | done | failed | unknown, straight from the server."""
    try:
        done = subprocess.run(_ssh_command(host, identity, f"status {batch_id}"),
                              capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return "unknown"
    return (done.stdout or "unknown").strip() or "unknown"


def import_result(api_base: str, batch_id: str, timeout: int = 20) -> Optional[dict[str, Any]]:
    """What the database says happened to a delivery, or None if it cannot say."""
    if not api_base:
        return None
    try:
        with urllib.request.urlopen(f"{api_base}/imports/{batch_id}", timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError):
        return None


def is_final(result: Optional[dict[str, Any]]) -> bool:
    return bool(result) and str(result.get("status")) in FINAL_STATUSES


def outcome_by_drive(result: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    """The per-drive outcomes from an import, keyed by (drive tag, view)."""
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for item in (result.get("items") or []):
        key = (str(item.get("drive_tag")), str(item.get("view") or "front"))
        out[key] = item
    return out
