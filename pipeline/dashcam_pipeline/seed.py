"""Telling the ledger what already exists.

Without this the first card would look like a machine that has never seen a
dashcam: every drive already in the database would be planned as new, processed
again, and delivered again. Seeding reads what is already true --

* the drives the server already has, from its own API;
* the drive folders already processed on this PC, from their manifests --

and records them as known, with the clips each one is made of, so the planner
recognises them.

It is safe to run more than once: everything it writes is keyed by vehicle and
drive tag.
"""
from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Optional

#: The _02 / _03 suffix the old processing script appended on a re-run.
TRAILING_RUN_RE = re.compile(r"_\d{2}$")

from .config import Config
from .ledger import Ledger


@dataclass
class SeedReport:
    drives_from_server: int = 0
    drives_from_folders: int = 0
    clips_linked: int = 0
    distinct_drives: int = 0
    folders_without_manifest: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        """How many drives the ledger holds, not the sum of the two passes."""
        return self.distinct_drives or (self.drives_from_server + self.drives_from_folders)


def _fetch_sessions(api_base: str, timeout: int = 30) -> list[dict]:
    if not api_base:
        return []
    try:
        with urllib.request.urlopen(f"{api_base}/sessions?limit=5000", timeout=timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise RuntimeError(f"could not ask the server what it has: {exc}") from exc
    return data if isinstance(data, list) else []


def seed_from_server(
    cfg: Config, ledger: Ledger, *, log: Callable[[str], None] = print
) -> tuple[int, Optional[str]]:
    """Record every drive the database already holds."""
    try:
        sessions = _fetch_sessions(cfg.api_base)
    except RuntimeError as exc:
        log(str(exc))
        return 0, str(exc)

    recorded = 0
    for s in sessions:
        vehicle = s.get("vehicle_tag")
        tag = s.get("drive_tag")
        if not vehicle or not tag:
            continue  # a session from before drive tags existed
        ledger.record_drive(
            str(vehicle), str(tag), view="front",
            status="ingested",
            drive_session_id=s.get("drive_session_id"),
        )
        recorded += 1

    log(f"recorded {recorded} drive(s) the server already has")
    return recorded, None


def seed_from_folders(
    cfg: Config,
    ledger: Ledger,
    roots: Optional[Iterable[Path]] = None,
    *,
    log: Callable[[str], None] = print,
) -> tuple[int, int, list[str]]:
    """Record the drive folders already processed on this PC, and their clips.

    The clip list matters more than the drive itself: it is what lets the planner
    recognise the same clips turning up again on a card that was never erased.
    """
    if roots is None:
        roots = [v.output_root / v.tag for v in cfg.vehicles]

    drives = clips = noted = 0
    skipped: list[str] = []

    for root in roots:
        root = Path(root)
        if not root.is_dir():
            continue
        for folder in sorted(p for p in root.iterdir() if p.is_dir()):
            if folder.name.startswith("_"):
                continue
            manifest_path = folder / "manifest.json"
            if not manifest_path.is_file():
                skipped.append(str(folder))
                continue
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                skipped.append(str(folder))
                continue

            name, view = folder.name, "front"
            for candidate in ("front", "rear", "interior", "other"):
                if name.endswith(f"_{candidate}"):
                    name, view = name[: -(len(candidate) + 1)], candidate
                    break

            vehicle = str(manifest.get("vehicle_tag") or "")
            tag = str(manifest.get("drive_tag") or name)
            if not vehicle:
                skipped.append(str(folder))
                continue

            # Folders processed by the old script carry an _02 / _03 re-run
            # suffix in their tag. The database derives a drive's identity from
            # the tag with that suffix removed, so recording it verbatim would
            # give one real drive two rows here and have the planner rebuild a
            # drive that is already loaded.
            tag = TRAILING_RUN_RE.sub("", tag)

            stems = [
                str(c.get("clip_name"))
                for c in (manifest.get("clips") or [])
                if isinstance(c, dict) and c.get("clip_name")
            ]

            existing = ledger.get_drive(vehicle, tag, view)
            status = existing["status"] if existing else "processed"
            ledger.record_drive(
                vehicle, tag, view=view,
                status=status,
                drive_session_id=manifest.get("drive_session_id"),
                clip_set_sha1=manifest.get("clip_set_sha1"),
                clip_pairs=sum(1 for c in (manifest.get("clips") or [])
                               if isinstance(c, dict) and c.get("channel") == "front"),
                first_clip=stems[0] if stems else None,
                last_clip=stems[-1] if stems else None,
                folder=str(folder),
            )
            if stems:
                ledger.set_drive_clips(vehicle, tag, stems)
                clips += len(stems)
                # Also note each clip itself, so a card that was never erased is
                # recognised clip by clip and not copied all over again. Without
                # this the planner knows the drive is done, but only after the
                # whole card has been copied to find that out.
                noted += ledger.record_archived_clips(vehicle, stems)
            drives += 1

    log(f"recorded {drives} drive folder(s) on this PC, covering {clips} clip(s)")
    if noted:
        log(f"noted {noted} clip(s) as already processed, so they are never copied again")
    if skipped:
        log(f"{len(skipped)} folder(s) had no usable manifest and were left alone")
    return drives, clips, skipped


def seed(
    cfg: Config,
    ledger: Ledger,
    *,
    roots: Optional[Iterable[Path]] = None,
    log: Callable[[str], None] = print,
) -> SeedReport:
    """Both halves, server first so folder data can fill in the detail."""
    report = SeedReport()

    with ledger.transaction():
        recorded, error = seed_from_server(cfg, ledger, log=log)
        report.drives_from_server = recorded
        if error:
            report.errors.append(error)

        drives, clips, skipped = seed_from_folders(cfg, ledger, roots, log=log)
        report.drives_from_folders = drives
        report.clips_linked = clips
        report.folders_without_manifest = skipped

    # Most folders describe a drive the server already told us about, so the two
    # passes overlap. Report what the ledger actually holds rather than the sum.
    report.distinct_drives = sum(ledger.counts()["drives"].values())
    log(f"the ledger now holds {report.distinct_drives} drive(s)")
    return report
