"""Package processed drive folders into a delivery and hand it to the server.

The full pipeline will do this itself; this is the same delivery format in a form
that can be run by hand, which is how the backlog gets loaded and how the path
gets tested.

    python pipeline/ship_folder.py \
        --drive "F:\\Dashcam\\Processed_Camry\\camry\\20260806_094436_camry_front" \
        --vehicle camry --source-kind folder \
        --send kpennock@192.168.1.164 --identity ~/.ssh/dashcam_pipeline_ed25519

    ... --out C:\\temp\\batch.tar        # build it without sending

A delivery is a tar holding:

    batch.json                       what this is, and what is in it
    SHA256SUMS                       every file below, checksummed
    <vehicle>/<drive folder>/...     the drives themselves

The server's receiving script unpacks it into a staging folder, verifies the
checksums, and only then makes it visible to the importer, so a half-delivered
batch is never loaded.

The video clips under ``artifacts/clips`` are left out: the server has no use for
them and they are the bulk of the folder.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tarfile
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

#: Never worth sending: the raw clips and the per-clip sidecars they came with.
SKIP_DIRS = {"clips"}


def log(msg: str) -> None:
    print(msg, flush=True)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def wanted_files(drive: Path) -> list[Path]:
    """Every file in a drive folder except the raw clip artifacts."""
    out = []
    for p in sorted(drive.rglob("*")):
        if p.is_dir():
            continue
        if any(part in SKIP_DIRS for part in p.relative_to(drive).parts):
            continue
        out.append(p)
    return out


def read_manifest(drive: Path) -> dict[str, Any]:
    path = drive / "manifest.json"
    if not path.is_file():
        raise SystemExit(f"{drive} has no manifest.json, so there is nothing to send")
    return json.loads(path.read_text(encoding="utf-8"))


def describe_drive(drive: Path, vehicle_default: str) -> dict[str, Any]:
    """The batch.json entry for one drive folder."""
    manifest = read_manifest(drive)
    clips = [c for c in (manifest.get("clips") or []) if isinstance(c, dict)]
    fronts = sorted(
        str(c.get("clip_name")) for c in clips if str(c.get("channel")) == "front"
    )
    vehicle = manifest.get("vehicle_tag") or vehicle_default

    tag = manifest.get("drive_tag")
    view = "front"
    name = drive.name
    for candidate in ("front", "rear", "interior", "other"):
        if name.endswith(f"_{candidate}"):
            view = candidate
            name = name[: -(len(candidate) + 1)]
            break
    tag = tag or name

    return {
        "vehicle_tag": vehicle,
        "drive_tag": tag,
        "view": view,
        "folder": f"{vehicle}/{drive.name}",
        "status": "processed",
        "clip_pairs": len(fronts),
        "first_clip": fronts[0] if fronts else None,
        "last_clip": fronts[-1] if fronts else None,
        "counts": {
            "clips": len(clips),
            "gnss_rows": ((manifest.get("telemetry") or {}).get("gnss") or {}).get("rows"),
            "accel_rows": ((manifest.get("telemetry") or {}).get("accel") or {}).get("rows"),
        },
        "time_confidence": (manifest.get("session_time") or {}).get("time_confidence"),
    }


def build_batch(
    drives: Iterable[Path],
    *,
    vehicle_default: str,
    source_kind: str,
    source_key: Optional[str],
    batch_id: str,
    staging: Path,
) -> dict[str, Any]:
    """Lay the delivery out in a staging folder and return its batch.json."""
    items: list[dict[str, Any]] = []
    checksums: list[tuple[str, str]] = []
    total_bytes = 0
    camera: dict[str, Any] = {}

    for drive in drives:
        manifest = read_manifest(drive)
        item = describe_drive(drive, vehicle_default)
        vehicle = item["vehicle_tag"]
        source = manifest.get("source") or {}
        camera = camera or {
            "serial": source.get("serial"),
            "model": source.get("model"),
            "firmware": source.get("firmware"),
        }

        dest_root = staging / vehicle / drive.name
        for src in wanted_files(drive):
            rel = src.relative_to(drive)
            dest = dest_root / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(src.read_bytes())
            total_bytes += dest.stat().st_size
            checksums.append((sha256_of(dest), f"{vehicle}/{drive.name}/{rel.as_posix()}"))

        items.append(item)
        log(f"  packed {drive.name}: {item['clip_pairs']} clip pairs")

    batch = {
        "batch_id": batch_id,
        "source_kind": source_kind,
        "source_key": source_key,
        "vehicle_tag": items[0]["vehicle_tag"] if items else vehicle_default,
        "camera": camera,
        "clips_new": sum(i["clip_pairs"] or 0 for i in items),
        "clips_on_source": None,
        "bytes_new": total_bytes,
        "detected_at": None,
        "acquired_at": None,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "items": items,
        "pc_summary": {
            "sent_by": "pipeline/ship_folder.py",
            "sent_utc": datetime.now(timezone.utc).isoformat(),
            "drives": len(items),
        },
    }

    (staging / "batch.json").write_text(json.dumps(batch, indent=2), encoding="utf-8")
    checksums.append((sha256_of(staging / "batch.json"), "batch.json"))
    (staging / "SHA256SUMS").write_text(
        "".join(f"{digest}  {name}\n" for digest, name in sorted(checksums, key=lambda t: t[1])),
        encoding="utf-8",
    )
    return batch


def write_tar(staging: Path, out: Path) -> int:
    with tarfile.open(out, "w") as tar:
        for path in sorted(staging.rglob("*")):
            if path.is_file():
                tar.add(path, arcname=path.relative_to(staging).as_posix())
    return out.stat().st_size


def send(tar_path: Path, *, host: str, identity: Optional[str], batch_id: str) -> int:
    """Stream the delivery to the server's receiving script."""
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new"]
    if identity:
        cmd += ["-i", str(Path(identity).expanduser())]
    cmd += [host, f"receive {batch_id}"]

    log(f"  sending to {host} ...")
    with tar_path.open("rb") as f:
        done = subprocess.run(cmd, stdin=f, capture_output=True, text=True)
    if done.stdout.strip():
        log(f"  server said: {done.stdout.strip()}")
    if done.returncode != 0:
        log(f"  the server refused it: {done.stderr.strip()}")
    return done.returncode


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Package processed drive folders and send them to the server.")
    ap.add_argument("--drive", action="append", dest="drives", required=True, type=Path,
                    help="A processed drive folder. Repeatable.")
    ap.add_argument("--vehicle", default=None,
                    help="Vehicle tag to use when a manifest does not state one.")
    ap.add_argument("--source-kind", default="folder", choices=["sdcard", "folder", "wifi", "legacy"])
    ap.add_argument("--source-key", default=None, help="Where these came from, for the record.")
    ap.add_argument("--batch-id", default=None, help="Delivery id (default: a new one).")
    ap.add_argument("--out", type=Path, default=None, help="Write the delivery here instead of sending it.")
    ap.add_argument("--send", default=None, metavar="USER@HOST", help="Send the delivery to this server.")
    ap.add_argument("--identity", default=None, help="SSH key to send with.")
    args = ap.parse_args(argv)

    if not args.out and not args.send:
        raise SystemExit("Nothing to do: pass --send USER@HOST, or --out to build the delivery only.")

    drives = [Path(d).resolve() for d in args.drives]
    for d in drives:
        if not d.is_dir():
            raise SystemExit(f"not a folder: {d}")

    batch_id = args.batch_id or str(uuid.uuid4())

    # A drive folder is named <date>_<time>_<vehicle>_<view>. Fall back to that
    # only when --vehicle was not given; a manifest that names the vehicle wins
    # over both, per drive, in describe_drive().
    vehicle_default = args.vehicle
    if not vehicle_default:
        parts = drives[0].name.split("_")
        vehicle_default = parts[2] if len(parts) > 2 else "unknown"

    log(f"batch {batch_id}: {len(drives)} drive(s)")
    with tempfile.TemporaryDirectory(prefix="dashcam-batch-") as tmp:
        staging = Path(tmp) / batch_id
        staging.mkdir(parents=True)
        batch = build_batch(
            drives,
            vehicle_default=vehicle_default,
            source_kind=args.source_kind,
            source_key=args.source_key,
            batch_id=batch_id,
            staging=staging,
        )

        tar_path = Path(args.out) if args.out else Path(tmp) / f"{batch_id}.tar"
        size = write_tar(staging, tar_path)
        log(f"  delivery is {size / 1e6:.1f} MB, {batch['clips_new']} clip pairs")

        if args.out and not args.send:
            log(f"  written to {tar_path}")
            return 0

        rc = send(tar_path, host=args.send, identity=args.identity, batch_id=batch_id)

    print(json.dumps({"batch_id": batch_id, "drives": len(drives), "sent": rc == 0}, indent=2))
    return rc


if __name__ == "__main__":
    sys.exit(main())
