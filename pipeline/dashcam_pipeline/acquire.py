"""Reading a card without copying it.

The database needs each clip's telemetry block and a couple of frames, which is
under three percent of the file, so nothing is copied off the card any more.
New clips are noted in the ledger and the drives script reads them where they
are. Nothing is ever written to the card: the telemetry reader works in-process
and everything it produces goes into the drive's own work folder.

What is left for this module is the bookkeeping around that: which car a card
belongs to, which of its clips are new, and where a clip can be read from right
now.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping, Optional

from .config import Config, Vehicle
from .ledger import Ledger
from .sources import ClipRef, Source


@dataclass
class AcquireResult:
    """What one look at a source found."""

    vehicle_tag: str
    source_kind: str
    source_key: str
    clips: list[ClipRef] = field(default_factory=list)     # everything on the source now
    new_stems: list[str] = field(default_factory=list)     # not in the ledger before this look
    detected_at: str = ""
    finished_at: str = ""

    @property
    def clips_on_source(self) -> int:
        return len(self.clips)

    @property
    def clips_new(self) -> int:
        return len(self.new_stems)

    @property
    def on_source(self) -> dict[str, Path]:
        """Where each clip on the source can be read from, by clip name."""
        return {c.stem: c.path for c in self.clips}


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def is_known(ledger: Ledger, vehicle_tag: str, stem: str) -> bool:
    """Has this clip been dealt with before, in any way?

    Any row counts: read off a card (seen), copied in the old copy-first flow
    (staged, held, purged), or noted as processed before this pipeline existed
    (archived). A clip that is known but whose drive never finished is brought
    back into planning from the drive's own record, not from here.
    """
    return ledger.get_clip(vehicle_tag, stem) is not None


def register_new_clips(
    ledger: Ledger,
    source: Source,
    vehicle: Vehicle,
    *,
    log: Callable[[str], None] = print,
    detected_at: Optional[str] = None,
) -> AcquireResult:
    """Note what is on the source. Copy nothing."""
    result = AcquireResult(
        vehicle_tag=vehicle.tag,
        source_kind=source.kind,
        source_key=source.key,
        detected_at=detected_at or utcnow(),
    )
    result.clips = source.list_clips()
    new = [c for c in result.clips if not is_known(ledger, vehicle.tag, c.stem)]

    with ledger.transaction():
        for clip in new:
            ledger.record_clip(
                vehicle_tag=vehicle.tag,
                stem=clip.stem,
                ts_key=clip.ts_key,
                channel=clip.channel,
                clip_type=clip.clip_type,
                size_bytes=clip.size_bytes,
                source_kind=source.kind,
                source_key=source.key,
                state="seen",
            )
    result.new_stems = [c.stem for c in new]
    result.finished_at = utcnow()

    known = len(result.clips) - len(new)
    if new:
        log(f"{vehicle.tag}: {len(new)} new clip(s) on {source.key} ({known} already known)")
    else:
        log(f"{vehicle.tag}: nothing new on {source.key} ({known} clips already known)")
    return result


def clip_paths(
    ledger: Ledger,
    vehicle_tag: str,
    stems: Iterable[str],
    on_source: Mapping[str, Path],
) -> dict[str, Path]:
    """Where each of these clips can be read from right now.

    The source itself comes first. Failing that, a copy left over from the
    copy-first days (staged or held) will do. A clip in neither place is left
    out, and the caller decides what that means for its drive.
    """
    out: dict[str, Path] = {}
    for stem in stems:
        candidate = on_source.get(stem)
        if candidate is not None and Path(candidate).is_file():
            out[stem] = Path(candidate)
            continue
        row = ledger.get_clip(vehicle_tag, stem)
        if row is None:
            continue
        for column in ("staged_path", "held_path"):
            value = row[column]
            if value and Path(value).is_file():
                out[stem] = Path(value)
                break
    return out


def identify_source(
    cfg: Config, source: Source, *, log: Callable[[str], None] = print
) -> tuple[Optional[Vehicle], Optional[str]]:
    """Which car's card is this? Returns (vehicle, serial as read)."""
    serial = source.identify([v.serial for v in cfg.vehicles])
    if not serial:
        log(f"no camera serial could be read from {source.key}")
        return None, None
    vehicle = cfg.vehicle_by_serial(serial)
    if vehicle is None:
        log(f"{source.key} holds footage from {serial}, which is not a configured car")
    return vehicle, serial
