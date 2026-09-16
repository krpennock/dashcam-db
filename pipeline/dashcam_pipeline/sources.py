"""Where clips come from, and how they are read.

Today that is a camera card in a reader. A folder of already-copied clips uses
the same interface, which is how the backlog is handled, and a Wi-Fi source can
be added later without anything else changing.

**A card is never written to.** No sidecar files, no deletions, no renames: the
pipeline copies what it needs and tells you when the card is safe to erase. That
is why processing always happens on the copy — the telemetry tool writes its
output next to the file it reads, so pointing it at a card would write to the
card.
"""
from __future__ import annotations

import ctypes
import datetime as _dt
import hashlib
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional, Protocol, runtime_checkable

#: BlackVue clips are named YYYYMMDD_HHMMSS_<type><F|R>.mp4
CLIP_RE = re.compile(
    r"^(?P<date>\d{8})_(?P<time>\d{6})_(?P<type>[A-Z]+)(?P<view>[FR])\.mp4$", re.IGNORECASE
)

#: Camera serials look like ELT9K1OBE00076, and are embedded near the start of
#: every clip the camera writes.
SERIAL_RE = re.compile(rb"ELT[0-9A-Z]{11}")

#: How much of a clip to read when looking for the serial, and when fingerprinting.
SERIAL_SCAN_BYTES = 4 * 1024 * 1024
FINGERPRINT_BYTES = 1024 * 1024

DRIVE_REMOVABLE = 2
DRIVE_FIXED = 3


@dataclass(frozen=True)
class ClipRef:
    """One clip on a source, before it has been copied anywhere."""

    path: Path
    stem: str
    ts_key: str
    ts_dt: _dt.datetime
    channel: str          # front | rear
    clip_type: str        # N E P I M
    size_bytes: int

    @property
    def name(self) -> str:
        return self.path.name


def parse_clip_name(name: str) -> Optional[tuple[str, _dt.datetime, str, str]]:
    """(ts_key, ts_dt, channel, type) for a clip filename, or None."""
    m = CLIP_RE.match(name)
    if not m:
        return None
    ts_key = f"{m.group('date')}_{m.group('time')}"
    try:
        ts_dt = _dt.datetime.strptime(ts_key, "%Y%m%d_%H%M%S")
    except ValueError:
        return None
    channel = "front" if m.group("view").upper() == "F" else "rear"
    return ts_key, ts_dt, channel, m.group("type").upper()


def fingerprint(path: Path) -> tuple[Optional[str], Optional[str]]:
    """A cheap identity for a clip: the first and last megabyte, hashed.

    Reading a whole 200 MB clip to notice it is the same file we already have
    would make re-inserting a card slow for no gain; the ends plus the size are
    enough to catch a truncated or replaced file.
    """
    try:
        size = path.stat().st_size
        with path.open("rb") as f:
            head = hashlib.sha1(f.read(FINGERPRINT_BYTES)).hexdigest()
            if size > FINGERPRINT_BYTES:
                f.seek(max(0, size - FINGERPRINT_BYTES))
                tail = hashlib.sha1(f.read(FINGERPRINT_BYTES)).hexdigest()
            else:
                tail = head
        return head, tail
    except OSError:
        return None, None


def read_serial(path: Path, known: Iterable[str] = ()) -> Optional[str]:
    """The camera serial recorded inside a clip.

    A configured serial wins if one appears; otherwise the first serial-shaped
    string found is returned, so an unknown camera can be reported by name rather
    than as a mystery.
    """
    wanted = {s.strip().upper().encode() for s in known if s}
    try:
        with path.open("rb") as f:
            blob = f.read(SERIAL_SCAN_BYTES)
    except OSError:
        return None

    for serial in wanted:
        if serial in blob:
            return serial.decode()
    m = SERIAL_RE.search(blob)
    return m.group(0).decode() if m else None


# --------------------------------------------------------------------------
# The seam
# --------------------------------------------------------------------------

@runtime_checkable
class Source(Protocol):
    """Somewhere clips can be read from."""

    kind: str           # sdcard | folder | wifi
    key: str            # which card, which folder, which camera
    erase_semantics: str  # "safe_to_erase" once copied, or "leave_alone"

    def is_present(self) -> bool: ...
    def identify(self, known_serials: Iterable[str] = ()) -> Optional[str]: ...
    def list_clips(self) -> list[ClipRef]: ...
    def acquire(self, clip: ClipRef, dest: Path) -> Path: ...


class _FileSource:
    """Shared behaviour for anything that is a folder of clips underneath."""

    kind = "folder"
    erase_semantics = "leave_alone"

    def __init__(self, root: Path, *, key: Optional[str] = None, settle_seconds: float = 2.0):
        self.root = Path(root)
        self.key = key or str(self.root)
        self.settle_seconds = settle_seconds

    def is_present(self) -> bool:
        return self.root.is_dir()

    def _mp4s(self) -> Iterator[Path]:
        if not self.root.is_dir():
            return iter(())
        return (p for p in sorted(self.root.iterdir()) if p.is_file() and p.suffix.lower() == ".mp4")

    def list_clips(self) -> list[ClipRef]:
        """Every clip that is complete enough to copy.

        A file still being written by the camera is skipped: its size is compared
        a moment apart, and only a file that has stopped changing is offered.
        """
        first: dict[Path, int] = {}
        for p in self._mp4s():
            try:
                first[p] = p.stat().st_size
            except OSError:
                continue

        import time
        if self.settle_seconds:
            time.sleep(self.settle_seconds)

        clips: list[ClipRef] = []
        for p, size in first.items():
            parsed = parse_clip_name(p.name)
            if not parsed:
                continue
            try:
                if p.stat().st_size != size:
                    continue  # still growing
            except OSError:
                continue
            ts_key, ts_dt, channel, clip_type = parsed
            clips.append(
                ClipRef(path=p, stem=p.stem, ts_key=ts_key, ts_dt=ts_dt,
                        channel=channel, clip_type=clip_type, size_bytes=size)
            )
        clips.sort(key=lambda c: (c.ts_dt, c.channel))
        return clips

    def identify(self, known_serials: Iterable[str] = ()) -> Optional[str]:
        """Read the serial from up to five clips, newest first."""
        clips = self.list_clips()
        for clip in list(reversed(clips))[:5]:
            serial = read_serial(clip.path, known_serials)
            if serial:
                return serial
        return None

    def acquire(self, clip: ClipRef, dest: Path) -> Path:
        """Copy one clip, and only rename it into place once it is whole.

        The copy lands as ``<name>.partial`` first, so a card pulled mid-copy
        leaves an obviously incomplete file rather than a short clip that looks
        finished.
        """
        dest.parent.mkdir(parents=True, exist_ok=True)
        final = dest if dest.suffix.lower() == ".mp4" else dest / clip.name
        partial = final.with_name(final.name + ".partial")
        if partial.exists():
            partial.unlink()

        shutil.copyfile(clip.path, partial)
        copied = partial.stat().st_size
        if copied != clip.size_bytes:
            partial.unlink(missing_ok=True)
            raise IOError(
                f"{clip.name}: copied {copied} bytes but the source has {clip.size_bytes}"
            )
        partial.replace(final)
        return final


class FolderSource(_FileSource):
    """A plain folder of clips already on this machine.

    Used for the backlog dumps, and for re-processing something by hand. Nothing
    is ever deleted from it.
    """

    kind = "folder"
    erase_semantics = "leave_alone"


class SdCardSource(_FileSource):
    """A camera card in a reader.

    Read-only, always: the pipeline copies what is new and then says the card is
    safe to erase, leaving the erasing to a person (or to the camera).
    """

    kind = "sdcard"
    erase_semantics = "safe_to_erase"

    def __init__(self, volume: Path, subpath: str = r"BlackVue\Record", **kwargs: object):
        self.volume = Path(volume)
        super().__init__(self.volume / subpath, key=str(self.volume), **kwargs)  # type: ignore[arg-type]

    def is_present(self) -> bool:
        return self.volume.exists() and self.root.is_dir()


# --------------------------------------------------------------------------
# Finding cards
# --------------------------------------------------------------------------

def removable_volumes() -> list[Path]:
    """Drive letters Windows considers removable.

    ctypes rather than a third-party package: this has to keep working on a
    machine nobody has touched in months.
    """
    try:
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    except AttributeError:
        return []  # not Windows

    out: list[Path] = []
    mask = kernel32.GetLogicalDrives()
    for i in range(26):
        if not (mask >> i) & 1:
            continue
        letter = f"{chr(ord('A') + i)}:"
        try:
            if kernel32.GetDriveTypeW(f"{letter}\\") == DRIVE_REMOVABLE:
                out.append(Path(f"{letter}\\"))
        except OSError:
            continue
    return out


def find_card_sources(
    subpath: str = r"BlackVue\Record",
    exclude_volumes: Iterable[str] = (),
) -> list[SdCardSource]:
    """Every removable volume that looks like a dashcam card.

    A volume is only a candidate if it actually holds the camera's folder, so a
    USB stick or a phone in the same reader is ignored rather than scanned.
    """
    excluded = {str(v).rstrip("\\").upper() for v in exclude_volumes}
    found: list[SdCardSource] = []
    for volume in removable_volumes():
        letter = str(volume).rstrip("\\").upper()
        if letter in excluded:
            continue
        source = SdCardSource(volume, subpath)
        if source.is_present():
            found.append(source)
    return found
