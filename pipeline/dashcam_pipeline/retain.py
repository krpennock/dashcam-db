"""Keeping the raw video for a while, then letting it go.

Processed drives are small; the video they came from is not. Clips are moved out
of staging into the holding folder once their drive is built, and the holding
folder is kept under control by age and by size: anything older than the window
goes, and if it is still over the cap the oldest go until it is not.

Three things are never deleted, whatever the numbers say:

* a clip belonging to a drive that has not reached the database -- losing the
  video of a drive that failed to load would be the worst outcome here;
* a clip whose filename appears anywhere under the "keep" folders, which is how
  you mark footage worth keeping;
* anything at all, when `--dry-run` is asked for.
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional, Sequence

from .config import Config
from .ledger import Ledger


@dataclass
class PruneReport:
    considered: int = 0
    deleted: int = 0
    bytes_freed: int = 0
    kept_for_drives: int = 0
    kept_by_name: int = 0
    kept_unclaimed: int = 0
    missing: int = 0
    dry_run: bool = False
    held_bytes_before: int = 0
    held_bytes_after: int = 0
    deleted_stems: list[str] = field(default_factory=list)

    @property
    def gigabytes_freed(self) -> float:
        return self.bytes_freed / (1024 ** 3)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def holding_dir_for(cfg: Config, vehicle_tag: str, ts_key: str) -> Path:
    """Holding is laid out by vehicle and day, so it stays browsable by hand."""
    return cfg.holding_path / vehicle_tag / ts_key[:8]


def keep_names(cfg: Config) -> set[str]:
    """Filenames marked as worth keeping, by being under a keep folder.

    Matched by name rather than by path: the same clip lives in holding and, if
    you have copied it somewhere interesting, there as well.
    """
    names: set[str] = set()
    for root in cfg.keep_dirs:
        if not root.is_dir():
            continue
        for p in root.rglob("*.mp4"):
            names.add(p.name.lower())
    return names


def move_to_holding(
    cfg: Config,
    ledger: Ledger,
    vehicle_tag: str,
    stems: Iterable[str],
    *,
    log: Callable[[str], None] = print,
) -> int:
    """Move staged clips into the holding folder, recording where they went.

    On the same disk this is a rename; onto a share it is a copy, verified by
    size, and only then is the staged copy removed.
    """
    moved = 0
    for stem in stems:
        row = ledger.get_clip(vehicle_tag, stem)
        if row is None or not row["staged_path"]:
            continue
        source = Path(row["staged_path"])
        if not source.is_file():
            continue

        dest_dir = holding_dir_for(cfg, vehicle_tag, row["ts_key"])
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / source.name

        try:
            if dest.exists():
                source.unlink()
            elif source.drive.lower() == dest.drive.lower():
                source.replace(dest)
            else:
                shutil.copyfile(source, dest)
                if dest.stat().st_size != source.stat().st_size:
                    dest.unlink(missing_ok=True)
                    raise IOError(f"{source.name}: the copy in holding is a different size")
                source.unlink()
        except OSError as exc:
            log(f"{vehicle_tag}: {source.name} could not be moved into holding: {exc}")
            continue

        ledger.set_clip_state(vehicle_tag, stem, "held", staged_path=None, held_path=str(dest))
        moved += 1

    if moved:
        log(f"{vehicle_tag}: {moved} clip(s) moved into holding")
    return moved


def held_size_bytes(cfg: Config) -> int:
    total = 0
    if not cfg.holding_path.is_dir():
        return 0
    for p in cfg.holding_path.rglob("*.mp4"):
        try:
            total += p.stat().st_size
        except OSError:
            continue
    return total


def free_gb(path: Path) -> float:
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except OSError:
        return float("inf")


def prune(
    cfg: Config,
    ledger: Ledger,
    *,
    dry_run: bool = True,
    log: Callable[[str], None] = print,
) -> PruneReport:
    """Bring the holding folder back within its age and size limits."""
    report = PruneReport(dry_run=dry_run)
    report.held_bytes_before = held_size_bytes(cfg)

    protected = keep_names(cfg)
    cutoff = utcnow() - timedelta(days=cfg.holding_days)
    cap_bytes = int(cfg.holding_max_gb * (1024 ** 3))

    held = ledger.clips_in_state("held")
    report.considered = len(held)

    # Oldest first: a clip's own timestamp, not when we happened to copy it.
    ordered = sorted(held, key=lambda r: (r["ts_key"], r["stem"]))
    running = report.held_bytes_before

    for row in ordered:
        vehicle_tag, stem = row["vehicle_tag"], row["stem"]
        path = Path(row["held_path"]) if row["held_path"] else None

        if path is None or not path.is_file():
            report.missing += 1
            continue

        # Age is counted from when the clip was copied here, not from when it was
        # filmed. A card that has not been erased for months holds footage that is
        # already older than the window, and measuring by the recording date would
        # copy it off the card and delete it in the same breath.
        too_old = False
        acquired = row["acquired_at"]
        if acquired:
            try:
                when = datetime.fromisoformat(str(acquired))
                if when.tzinfo is None:
                    when = when.replace(tzinfo=timezone.utc)
                too_old = when < cutoff
            except ValueError:
                pass
        over_cap = running > cap_bytes
        if not (too_old or over_cap):
            continue

        if path.name.lower() in protected:
            report.kept_by_name += 1
            continue
        if not ledger.clip_is_safe_to_delete(vehicle_tag, stem):
            # Either its drive has not reached the database, or no drive claims
            # it at all. Both are reasons to keep the video; they are counted
            # apart so an unclaimed pile does not hide behind the ordinary case.
            if ledger.drives_for_clip(vehicle_tag, stem):
                report.kept_for_drives += 1
            else:
                report.kept_unclaimed += 1
            continue

        size = path.stat().st_size
        if dry_run:
            log(f"  would remove {path.name} ({size / 1e6:.0f} MB)")
        else:
            try:
                path.unlink()
            except OSError as exc:
                log(f"  {path.name} could not be removed: {exc}")
                continue
            ledger.set_clip_state(vehicle_tag, stem, "purged", held_path=None)

        report.deleted += 1
        report.bytes_freed += size
        report.deleted_stems.append(stem)
        running -= size

    report.held_bytes_after = running if not dry_run else report.held_bytes_before

    kept = (
        f"kept {report.kept_for_drives} still waiting on the database, "
        f"{report.kept_by_name} marked to keep"
    )
    if report.kept_unclaimed:
        kept += f", {report.kept_unclaimed} that no drive claims"
    log(
        f"holding: {report.held_bytes_before / (1024 ** 3):.0f} GB held, "
        f"{'would remove' if dry_run else 'removed'} {report.deleted} clip(s) "
        f"({report.gigabytes_freed:.1f} GB); {kept}"
    )
    return report


def should_warn(cfg: Config, report: PruneReport) -> bool:
    """Is holding still over its limits, or the disk low, after a prune?"""
    over_cap = report.held_bytes_after > int(cfg.holding_max_gb * (1024 ** 3))
    low_disk = free_gb(cfg.holding_path) < cfg.holding_min_free_gb
    return over_cap or low_disk
