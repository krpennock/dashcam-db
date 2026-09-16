"""Send processed drive folders to the server, by hand.

The watcher does this itself for a card; this is the same delivery, in a form you
can run against any folder of processed drives. It is how the backlog was loaded,
and how a drive gets re-sent after a problem.

    python pipeline/ship_folder.py \
        --drive "F:\\Dashcam\\Processed_Camry\\camry\\20260806_094436_camry_front" \
        --vehicle camry \
        --send kpennock@192.168.1.164 --identity ~/.ssh/dashcam_pipeline_ed25519

    ... --out C:\\temp\\batch.tar      # build the delivery without sending it

The delivery format itself lives in dashcam_pipeline/delivery.py, so there is one
definition of it rather than two that can drift apart.
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dashcam_pipeline import delivery  # noqa: E402


def log(msg: str) -> None:
    print(msg, flush=True)


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

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
        if not (d / "manifest.json").is_file():
            raise SystemExit(f"{d} has no manifest.json, so there is nothing to send")

    # A drive folder is named <date>_<time>_<vehicle>_<view>. Fall back to that
    # only when --vehicle was not given; a manifest naming the vehicle wins.
    vehicle = args.vehicle
    if not vehicle:
        parts = drives[0].name.split("_")
        vehicle = parts[2] if len(parts) > 2 else "unknown"

    with tempfile.TemporaryDirectory(prefix="dashcam-batch-") as tmp:
        built = delivery.build(
            drives,
            vehicle_tag=vehicle,
            source_kind=args.source_kind,
            source_key=args.source_key,
            batch_id=args.batch_id,
            workdir=Path(tmp),
        )
        log(f"batch {built.batch_id}: {len(drives)} drive(s), "
            f"{built.batch['clips_new']} clip pairs, {built.bytes_total / 1e6:.1f} MB")

        if args.out:
            shutil.copyfile(built.tar_path, args.out)
            log(f"  written to {args.out}")
            if not args.send:
                return 0

        identity = Path(args.identity).expanduser() if args.identity else None
        log(f"  sending to {args.send} ...")
        try:
            reply = delivery.send(built, host=args.send, identity=identity)
        except delivery.DeliveryError as exc:
            log(f"  the server refused it: {exc}")
            return 1
        log(f"  server said: {reply}")

    print(json.dumps({"batch_id": built.batch_id, "drives": len(drives), "sent": True}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
