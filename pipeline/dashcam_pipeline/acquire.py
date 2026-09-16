"""Getting clips off a card, without ever writing to it.

The card is read and nothing else: no sidecar files, no deletions, no renames.
Clips that are new to us are copied into the staging folder, each one verified by
size before it counts as copied, and only when every clip on the card is safely
here does the pipeline say the card is safe to erase.

Pulling the card mid-copy is expected, not an error: whatever arrived complete is
kept, the rest is left, and putting the card back in carries on where it stopped.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional, Sequence

from . import notify, sources
from .config import Config, Vehicle
from .ledger import Ledger
from .sources import ClipRef, Source


@dataclass
class AcquireResult:
    vehicle_tag: str
    source_kind: str
    source_key: str
    clips_on_source: int = 0
    clips_new: int = 0
    clips_copied: int = 0
    bytes_copied: int = 0
    copied_stems: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)
    interrupted: bool = False
    safe_to_erase: bool = False
    detected_at: str = ""
    finished_at: str = ""

    @property
    def gigabytes(self) -> float:
        return self.bytes_copied / (1024 ** 3)

    @property
    def remaining(self) -> int:
        return max(0, self.clips_new - self.clips_copied)


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def staging_dir_for(cfg: Config, vehicle_tag: str) -> Path:
    return cfg.staging_dir / vehicle_tag


def _already_have(ledger: Ledger, vehicle_tag: str, clip: ClipRef) -> bool:
    """Is this clip already recorded, and still where the ledger says it is?

    A clip whose staged copy has vanished (someone emptied the folder, a disk
    problem) is treated as new again, so the card can make good the gap while it
    is still in the reader.
    """
    row = ledger.get_clip(vehicle_tag, clip.stem)
    if row is None:
        return False
    if row["state"] in ("purged", "archived"):
        # Purged: deliberately deleted by the holding window. Archived: already
        # processed before this pipeline existed, its video kept elsewhere.
        # Either way it is accounted for and must not be fetched again.
        return True
    for column in ("staged_path", "held_path"):
        value = row[column]
        if value and Path(value).is_file():
            return True
    return False


def acquire_new_clips(
    cfg: Config,
    ledger: Ledger,
    source: Source,
    vehicle: Vehicle,
    *,
    log: Callable[[str], None] = print,
    detected_at: Optional[str] = None,
) -> AcquireResult:
    """Copy whatever is new on this source into staging."""
    result = AcquireResult(
        vehicle_tag=vehicle.tag,
        source_kind=source.kind,
        source_key=source.key,
        detected_at=detected_at or utcnow(),
    )

    clips = source.list_clips()
    result.clips_on_source = len(clips)
    new_clips = [c for c in clips if not _already_have(ledger, vehicle.tag, c)]
    result.clips_new = len(new_clips)

    if not new_clips:
        result.safe_to_erase = bool(clips) and source.erase_semantics == "safe_to_erase"
        result.finished_at = utcnow()
        log(f"{vehicle.tag}: nothing new on {source.key} ({len(clips)} clips already here)")
        return result

    dest_dir = staging_dir_for(cfg, vehicle.tag)
    dest_dir.mkdir(parents=True, exist_ok=True)
    log(f"{vehicle.tag}: {len(new_clips)} new clip(s) to copy from {source.key}")

    for clip in new_clips:
        try:
            staged = source.acquire(clip, dest_dir / clip.name)
        except (FileNotFoundError, PermissionError, OSError) as exc:
            # The card going away mid-copy is the common case, and is not a fault.
            if not source.is_present():
                result.interrupted = True
                log(f"{vehicle.tag}: the card went away after {result.clips_copied} clip(s)")
                break
            result.failed.append((clip.stem, f"{type(exc).__name__}: {exc}"))
            log(f"{vehicle.tag}: {clip.name} could not be copied: {exc}")
            continue

        head, tail = sources.fingerprint(staged)
        ledger.record_clip(
            vehicle_tag=vehicle.tag,
            stem=clip.stem,
            ts_key=clip.ts_key,
            channel=clip.channel,
            clip_type=clip.clip_type,
            size_bytes=clip.size_bytes,
            head_sha1=head,
            tail_sha1=tail,
            source_kind=source.kind,
            source_key=source.key,
            state="staged",
            staged_path=str(staged),
        )
        result.clips_copied += 1
        result.bytes_copied += clip.size_bytes
        result.copied_stems.append(clip.stem)

    result.finished_at = utcnow()

    # Only claim the card can be erased when every clip on it is accounted for.
    if source.erase_semantics == "safe_to_erase" and not result.interrupted:
        known = ledger.known_stems(vehicle.tag)
        result.safe_to_erase = all(c.stem in known for c in clips)

    log(
        f"{vehicle.tag}: copied {result.clips_copied} of {result.clips_new} "
        f"({result.gigabytes:.1f} GB)"
        + (", interrupted" if result.interrupted else "")
        + (", safe to erase" if result.safe_to_erase else "")
    )
    return result


def announce(ledger: Ledger, result: AcquireResult) -> None:
    """Queue the messages this acquisition deserves."""
    if result.interrupted:
        notify.card_removed(ledger, result.vehicle_tag, result.clips_copied, result.remaining)
        return
    if result.clips_new == 0:
        notify.nothing_new(ledger, result.vehicle_tag)
        return
    notify.card_copied(ledger, result.vehicle_tag, result.clips_copied, result.gigabytes)
    if result.safe_to_erase:
        notify.safe_to_erase(ledger, result.vehicle_tag, result.clips_on_source)


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


def staged_clips(ledger: Ledger, vehicle_tag: str) -> list[str]:
    """Clips copied but not yet moved to the holding folder."""
    return [row["stem"] for row in ledger.clips_in_state("staged", vehicle_tag)]


def staged_paths(ledger: Ledger, vehicle_tag: str, stems: Iterable[str]) -> dict[str, Path]:
    """Where each of these clips currently sits, staged or held."""
    out: dict[str, Path] = {}
    for stem in stems:
        row = ledger.get_clip(vehicle_tag, stem)
        if row is None:
            continue
        for column in ("staged_path", "held_path"):
            value = row[column]
            if value and Path(value).is_file():
                out[stem] = Path(value)
                break
    return out
