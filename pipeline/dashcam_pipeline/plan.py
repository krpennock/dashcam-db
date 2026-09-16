"""Working out which drives a pile of clips belongs to.

Two questions, and the second is the one that matters:

1. Which clips belong together? Clips are grouped by the gap between them, the
   same rule the drives script uses, so the pipeline and the script agree about
   where a drive starts and ends.

2. Is this a drive we already have? A card is not erased every time, so the same
   clips keep turning up. The answer decides whether a drive is built, left
   alone, rebuilt under its existing identity, or set aside for a person:

   * **new** -- none of these clips belong to a drive we know. Build it.
   * **unchanged** -- exactly the drive we already have. Do nothing.
   * **extended** -- the same drive plus clips we did not have before (the card
     was copied part-way through last time). Rebuild it under the *existing*
     tag, so it updates the drive in the database instead of becoming a second
     copy of it.
   * **remnant** -- fewer clips than we already have, because the camera has
     since overwritten the earliest ones. Do nothing; we have the better copy.
   * **needs_review** -- these clips span two drives we know about, or overlap
     one only partly. Something is odd about the grouping and a person should
     look before anything is rebuilt.

A drive's tag is ``YYYYMMDD_HHMMSS_<vehicle>`` from its first clip, and the
database id is derived from that tag, so keeping the tag stable is what keeps a
re-processed drive the same drive.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
from dataclasses import dataclass, field
from typing import Iterable, Optional, Sequence

from .ledger import Ledger

#: Grouping and classification defaults, kept in step with the drives script.
DEFAULT_GAP_SECONDS = 120
DEFAULT_MIN_PAIRS = 3
DEFAULT_DRIVING_TYPES = frozenset({"N", "E", "M", "I"})

NEW = "new"
UNCHANGED = "unchanged"
EXTENDED = "extended"
REMNANT = "remnant"
NEEDS_REVIEW = "needs_review"


@dataclass
class DrivePlan:
    """One group of clips, and what should happen to it."""

    vehicle_tag: str
    drive_tag: str
    view: str
    stems: list[str]
    clip_pairs: int
    first_clip: str
    last_clip: str
    match: str                      # NEW / UNCHANGED / EXTENDED / REMNANT / NEEDS_REVIEW
    classification: str             # drive | skipped_short | parking_only
    known_drive_tag: Optional[str] = None
    reason: Optional[str] = None
    types: dict[str, int] = field(default_factory=dict)

    @property
    def should_build(self) -> bool:
        """Only a genuinely new or extended driving group is worth processing."""
        return self.classification == "drive" and self.match in (NEW, EXTENDED)

    @property
    def clip_set_sha1(self) -> str:
        return clip_set_sha1(self.stems)


def clip_set_sha1(stems: Iterable[str]) -> str:
    """Order-independent hash of a clip set, matching the drives script."""
    h = hashlib.sha1()
    for stem in sorted({str(s) for s in stems}):
        h.update(stem.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _ts_of(stem: str) -> Optional[_dt.datetime]:
    try:
        return _dt.datetime.strptime(stem[:15], "%Y%m%d_%H%M%S")
    except (ValueError, IndexError):
        return None


def _type_of(stem: str) -> str:
    """The recording type letter of a clip: N, E, P, I or M."""
    tail = stem.split("_")[-1] if "_" in stem else stem
    return tail[:-1].upper() if len(tail) >= 2 else ""


def group_into_drives(
    stems: Sequence[str], gap_seconds: int = DEFAULT_GAP_SECONDS
) -> list[list[str]]:
    """Split clips into drives on the gap between consecutive timestamps.

    Front and rear clips share a timestamp, so grouping is done on distinct
    timestamps and the clips are gathered back afterwards; otherwise a pair would
    look like two recordings a moment apart.
    """
    by_ts: dict[str, list[str]] = {}
    for stem in stems:
        ts = _ts_of(stem)
        if ts is None:
            continue
        by_ts.setdefault(stem[:15], []).append(stem)

    groups: list[list[str]] = []
    current: list[str] = []
    previous: Optional[_dt.datetime] = None

    for key in sorted(by_ts):
        moment = _dt.datetime.strptime(key, "%Y%m%d_%H%M%S")
        if previous is not None and (moment - previous).total_seconds() > gap_seconds:
            if current:
                groups.append(current)
            current = []
        current.extend(sorted(by_ts[key]))
        previous = moment

    if current:
        groups.append(current)
    return groups


def classify(
    stems: Sequence[str],
    *,
    driving_types: Iterable[str] = DEFAULT_DRIVING_TYPES,
    min_pairs: int = DEFAULT_MIN_PAIRS,
) -> tuple[str, dict[str, int], int]:
    """Is this group a drive, a parking session, or too short to bother with?

    Returns (classification, counts by type, number of front clips).
    """
    driving = {t.upper() for t in driving_types}
    counts: dict[str, int] = {}
    fronts = 0
    for stem in stems:
        letter = _type_of(stem)
        counts[letter] = counts.get(letter, 0) + 1
        if stem.upper().endswith("F"):
            fronts += 1

    if not any(letter in driving for letter in counts):
        # Motion in a car park is worth keeping the video of, but it is not a
        # drive and there is nothing useful to put in the database.
        return "parking_only", counts, fronts
    if fronts < min_pairs:
        return "skipped_short", counts, fronts
    return "drive", counts, fronts


def drive_tag_for(vehicle_tag: str, stems: Sequence[str]) -> str:
    first = min((s for s in stems if _ts_of(s)), key=lambda s: s[:15], default="")
    return f"{first[:15]}_{vehicle_tag}" if first else f"unknown_{vehicle_tag}"


def match_known(
    ledger: Ledger, vehicle_tag: str, stems: Sequence[str], view: str = "front"
) -> tuple[str, Optional[str], Optional[str]]:
    """Compare a group against the drives we already know.

    Returns (match kind, the known drive's tag, a sentence explaining it).
    """
    wanted = {str(s) for s in stems}
    touched: dict[str, set[str]] = {}
    for stem in wanted:
        for row in ledger.drives_for_clip(vehicle_tag, stem):
            if row["view"] != view:
                continue
            tag = row["drive_tag"]
            if tag not in touched:
                touched[tag] = set(ledger.drive_stems(vehicle_tag, tag))

    if not touched:
        return NEW, None, None

    if len(touched) > 1:
        names = ", ".join(sorted(touched))
        return (
            NEEDS_REVIEW,
            None,
            f"these clips span {len(touched)} drives already recorded ({names}), "
            f"so the grouping does not agree with what is already here",
        )

    tag, known = next(iter(touched.items()))
    if wanted == known:
        return UNCHANGED, tag, None
    if wanted > known:
        added = len(wanted - known)
        return EXTENDED, tag, f"{added} clip(s) arrived that {tag} did not have"
    if wanted < known:
        missing = len(known - wanted)
        return REMNANT, tag, f"{missing} clip(s) of {tag} are no longer on the card"
    return (
        NEEDS_REVIEW,
        tag,
        f"these clips overlap {tag} without containing it, which should not happen",
    )


def plan_drives(
    ledger: Ledger,
    vehicle_tag: str,
    stems: Sequence[str],
    *,
    gap_seconds: int = DEFAULT_GAP_SECONDS,
    driving_types: Iterable[str] = DEFAULT_DRIVING_TYPES,
    min_pairs: int = DEFAULT_MIN_PAIRS,
    view: str = "front",
) -> list[DrivePlan]:
    """Turn a pile of clips into a list of decisions, one per drive."""
    plans: list[DrivePlan] = []

    for group in group_into_drives(stems, gap_seconds):
        classification, counts, fronts = classify(
            group, driving_types=driving_types, min_pairs=min_pairs
        )
        match, known_tag, reason = match_known(ledger, vehicle_tag, group, view)

        # An extended drive keeps the tag it already has: the database id comes
        # from the tag, and changing it is exactly how duplicate drives happened.
        tag = known_tag if (match in (EXTENDED, UNCHANGED, REMNANT) and known_tag) else drive_tag_for(vehicle_tag, group)

        ordered = sorted(group)
        plans.append(
            DrivePlan(
                vehicle_tag=vehicle_tag,
                drive_tag=tag,
                view=view,
                stems=ordered,
                clip_pairs=fronts,
                first_clip=ordered[0] if ordered else "",
                last_clip=ordered[-1] if ordered else "",
                match=match,
                classification=classification,
                known_drive_tag=known_tag,
                reason=reason,
                types=counts,
            )
        )

    return plans


def summarise(plans: Sequence[DrivePlan]) -> str:
    """One line describing a set of decisions, for a log or a notification."""
    if not plans:
        return "nothing to do"
    bits: list[str] = []
    build = [p for p in plans if p.should_build]
    if build:
        bits.append(f"{len(build)} to build")
    for kind, label in (
        (UNCHANGED, "already here"),
        (REMNANT, "partly overwritten"),
        (NEEDS_REVIEW, "needing a look"),
    ):
        n = sum(1 for p in plans if p.match == kind)
        if n:
            bits.append(f"{n} {label}")
    for kind, label in (("parking_only", "parked"), ("skipped_short", "too short")):
        n = sum(1 for p in plans if p.classification == kind)
        if n:
            bits.append(f"{n} {label}")
    return ", ".join(bits)
