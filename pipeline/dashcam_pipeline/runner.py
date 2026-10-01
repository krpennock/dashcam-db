"""One pass of the whole pipeline, and the loop that keeps taking passes.

A pass is the same whether it was started by a card appearing or by hand:

    identify the car -> note which clips are new -> work out which drives those
    clips belong to -> build the ones worth building, reading straight off the
    card -> deliver them -> ask the server what became of them -> say whether
    the card can be erased -> send the notifications that piled up along the way

Nothing is copied off the card. The database needs each clip's telemetry block
and a couple of frames, which the drives script reads in place, so a full card
is dealt with in minutes and the card only has to stay in until the server
confirms the load.

Every step records what it did in the ledger before moving on, so an interrupted
pass loses nothing: the next one picks up from whatever is recorded. Taking the
card out part-way is expected, not a fault: the drive being built goes back to
"planned" and is built again when the card returns.
"""
from __future__ import annotations

import ctypes
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping, Optional, Sequence

from . import acquire, delivery, notify, plan, process, retain, sources
from .acquire import AcquireResult
from .config import Config, Vehicle
from .ledger import Ledger
from .plan import DrivePlan
from .sources import FolderSource, Source

#: Only one pipeline at a time touches a card, a ledger or an output folder.
MUTEX_NAME = "Local\\DashcamPipeline"
ERROR_ALREADY_EXISTS = 183

#: Keep the machine awake while a pass is running, but let the screen sleep.
ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001

#: A card has to be seen twice before it is read, so a half-mounted volume is
#: not scanned mid-mount.
CONFIRM_POLLS = 2

#: How often to ask the server what became of what we sent and send anything
#: still queued for the phone. This happens whether or not a card is in the
#: reader: a card left in overnight must not stop the pipeline finishing the
#: work it has already done.
HOUSEKEEPING_SECONDS = 60.0

#: A function that lists the clips currently on a source, by its key, or None
#: when that source is no longer there. Used to decide whether a card can go.
CardStems = Callable[[str], Optional[list[str]]]


@dataclass
class PassResult:
    vehicle_tag: str = ""
    source_key: str = ""
    acquired: Optional[AcquireResult] = None
    plans: list[DrivePlan] = field(default_factory=list)
    built: list[process.ProcessResult] = field(default_factory=list)
    batch_id: Optional[str] = None
    delivered: bool = False
    interrupted: bool = False
    error: Optional[str] = None

    @property
    def summary(self) -> str:
        bits = []
        if self.acquired:
            bits.append(f"{self.acquired.clips_new} new clip(s)")
        if self.plans:
            bits.append(plan.summarise(self.plans))
        if self.interrupted:
            bits.append("card removed part-way")
        if self.batch_id:
            bits.append("delivered" if self.delivered else "delivery queued")
        if self.error:
            bits.append(f"error: {self.error}")
        return "; ".join(bits) or "nothing to do"


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------------
# Windows housekeeping
# --------------------------------------------------------------------------

def single_instance() -> Optional[object]:
    """Take the pipeline's mutex, or return None if another copy holds it."""
    try:
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    except AttributeError:
        return object()  # not Windows: nothing to coordinate with
    handle = kernel32.CreateMutexW(None, False, MUTEX_NAME)
    if not handle or kernel32.GetLastError() == ERROR_ALREADY_EXISTS:
        return None
    return handle


class KeepAwake:
    """Stop Windows sleeping in the middle of working through a card."""

    def __enter__(self) -> "KeepAwake":
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(  # type: ignore[attr-defined]
                ES_CONTINUOUS | ES_SYSTEM_REQUIRED
            )
        except AttributeError:
            pass
        return self

    def __exit__(self, *exc: object) -> None:
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)  # type: ignore[attr-defined]
        except AttributeError:
            pass


# --------------------------------------------------------------------------
# The pass
# --------------------------------------------------------------------------

def _clips_to_consider(ledger: Ledger, vehicle_tag: str, stems: Sequence[str]) -> list[str]:
    """These clips, plus any we already know from around the same time.

    A drive cut short last time, or a card read before the camera had finished
    its last clip, leaves a drive split across two visits; regrouping with the
    neighbours is what lets the planner see it as one drive and rebuild it under
    the identity it already has.
    """
    if not stems:
        return []
    window = timedelta(hours=6)
    out = set(stems)
    for stem in list(stems):
        try:
            moment = datetime.strptime(stem[:15], "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        rows = ledger.clips_near(
            vehicle_tag,
            (moment - window).strftime("%Y%m%d_%H%M%S"),
            (moment + window).strftime("%Y%m%d_%H%M%S"),
        )
        out.update(r["stem"] for r in rows if r["state"] != "purged")
    return sorted(out)


def _record_plan(ledger: Ledger, drive: DrivePlan) -> None:
    # A drive we already know is left exactly as it is. A card holding only part
    # of it says nothing new, and rewriting the row from that partial view would
    # replace the full clip list with the shorter one -- which is what makes held
    # video look unclaimed and eligible for deletion. It would also relabel a
    # loaded 13-pair drive as "too short" because two pairs happened to survive.
    if drive.match in (plan.UNCHANGED, plan.REMNANT):
        if ledger.get_drive(drive.vehicle_tag, drive.drive_tag, drive.view):
            return

    status = {
        "parking_only": "parking_only",
        "skipped_short": "skipped_short",
    }.get(drive.classification, "planned")
    if drive.match == plan.NEEDS_REVIEW:
        status = "needs_review"

    ledger.record_drive(
        drive.vehicle_tag, drive.drive_tag, view=drive.view,
        status=status,
        clip_set_sha1=drive.clip_set_sha1,
        clip_pairs=drive.clip_pairs,
        first_clip=drive.first_clip,
        last_clip=drive.last_clip,
        reason=drive.reason,
        drive_session_id=process.drive_session_id_for(drive.vehicle_tag, drive.drive_tag),
    )
    ledger.set_drive_clips(drive.vehicle_tag, drive.drive_tag, drive.stems)


def card_stems_for(present: Mapping[str, Source]) -> CardStems:
    """A lookup from source key to the clips on it now, for the erase check."""
    def lookup(source_key: str) -> Optional[list[str]]:
        source = present.get(source_key)
        if source is None or not source.is_present():
            return None
        try:
            return [c.stem for c in source.list_clips()]
        except OSError:
            return None
    return lookup


def run_source(
    cfg: Config,
    ledger: Ledger,
    source: Source,
    *,
    vehicle: Optional[Vehicle] = None,
    log: Callable[[str], None] = print,
) -> PassResult:
    """Take one source from first sight to delivered."""
    result = PassResult(source_key=source.key)

    if vehicle is None:
        vehicle, serial = acquire.identify_source(cfg, source, log=log)
        if vehicle is None:
            notify.unknown_camera(ledger, source.key, serial)
            notify.flush(cfg, ledger, log=log)
            result.error = f"unknown camera {serial or 'with no readable serial'}"
            return result
    result.vehicle_tag = vehicle.tag

    found = acquire.register_new_clips(ledger, source, vehicle, log=log)
    result.acquired = found
    on_source = found.on_source

    # Plan over what is new, what was planned earlier and never finished, and
    # whatever we already know from around the same time, so each drive is seen
    # whole rather than as the fragment this visit happens to show.
    stems = set(found.new_stems)
    stems.update(ledger.stems_of_drives(vehicle.tag, plan.UNFINISHED_STATUS))
    plans = plan.plan_drives(
        ledger, vehicle.tag, _clips_to_consider(ledger, vehicle.tag, sorted(stems)),
        gap_seconds=int(cfg.processing.get("gap_seconds") or 120),
        driving_types=cfg.driving_types,
        min_pairs=int(cfg.processing.get("min_drive_clips") or 3),
    )
    result.plans = plans
    for drive in plans:
        _record_plan(ledger, drive)
        if drive.match == plan.NEEDS_REVIEW:
            notify.needs_review(ledger, vehicle.tag, drive.drive_tag, drive.reason or "")
    if plans:
        log(f"{vehicle.tag}: {plan.summarise(plans)}")

    to_build = [d for d in plans if d.should_build]
    if found.new_stems:
        notify.card_found(ledger, vehicle.tag, found.clips_new, len(to_build))
    elif not to_build and source.erase_semantics == "safe_to_erase":
        waiting = ledger.unsettled_stems(vehicle.tag, list(on_source))
        notify.nothing_new(ledger, vehicle.tag, waiting=len(waiting))
    # Say so now rather than after the building, which takes a little while.
    notify.flush(cfg, ledger, log=log)

    built: list[process.ProcessResult] = []
    handled = 0
    for drive in to_build:
        if not source.is_present():
            result.interrupted = True
            break

        paths = acquire.clip_paths(ledger, vehicle.tag, drive.stems, on_source)
        missing = [s for s in drive.stems if s not in paths]
        if missing:
            # Part of this drive is gone from the card and was never copied, so
            # building it now would quietly shrink the drive in the database.
            reason = (f"{len(missing)} of its clips are no longer on the card and "
                      f"were never copied, so it cannot be built whole")
            ledger.set_drive_status(vehicle.tag, drive.drive_tag, "needs_review", view=drive.view,
                                    reason_code="CLIPS_UNAVAILABLE", reason=reason)
            notify.needs_review(ledger, vehicle.tag, drive.drive_tag, reason)
            log(f"{vehicle.tag}: {drive.drive_tag} set aside: {reason}")
            handled += 1
            continue

        outcome = process.process_drive(
            cfg, vehicle, drive, [paths[s] for s in drive.stems], log=log
        )
        if outcome.ok:
            built.append(outcome)
            ledger.set_drive_status(
                vehicle.tag, drive.drive_tag, "processed", view=drive.view,
                folder=str(outcome.folder) if outcome.folder else None,
                drive_session_id=outcome.drive_session_id,
            )
        elif not source.is_present():
            # The card went away under the build. Not a fault: leave the drive
            # planned so it is built again when the card comes back, and stop.
            ledger.set_drive_status(
                vehicle.tag, drive.drive_tag, plan.UNFINISHED_STATUS, view=drive.view,
                reason_code="CARD_REMOVED", reason="the card was taken out during the build",
            )
            log(f"{vehicle.tag}: the card went away while building {drive.drive_tag}")
            result.interrupted = True
            break
        else:
            built.append(outcome)
            ledger.set_drive_status(
                vehicle.tag, drive.drive_tag, "failed", view=drive.view,
                reason_code=outcome.reason_code, reason=outcome.reason,
            )
            notify.processing_failed(ledger, vehicle.tag, drive.drive_tag, outcome.reason or "")
        handled += 1
    result.built = built

    if result.interrupted:
        done = sum(1 for b in built if b.ok)
        notify.card_removed(ledger, vehicle.tag, done, len(to_build) - handled)

    deliver_outcome = deliver_pending(cfg, ledger, vehicle.tag, acquired=found, log=log)
    result.batch_id = deliver_outcome.get("batch_id")
    result.delivered = bool(deliver_outcome.get("delivered"))

    confirm_deliveries(cfg, ledger, log=log, card_stems=card_stems_for({source.key: source}))
    tidy_holding(cfg, ledger, vehicle_tag=vehicle.tag, log=log)
    notify.flush(cfg, ledger, log=log)
    return result


def deliver_pending(
    cfg: Config,
    ledger: Ledger,
    vehicle_tag: str,
    *,
    acquired: Optional[AcquireResult] = None,
    log: Callable[[str], None] = print,
) -> dict[str, object]:
    """Send every processed drive that has not been delivered yet."""
    # Parcels made up on an earlier run but never sent are discarded before a new
    # one is made, so whatever they held travels in this parcel instead of
    # waiting for the run after it. Anything actually sent is left alone: the
    # server may still be working through it.
    dropped = ledger.supersede_unshipped_batches(vehicle_tag)
    if dropped:
        log(f"{vehicle_tag}: {dropped} parcel(s) never left here; making a fresh one")

    ready = [
        row for row in ledger.drives_with_status("processed")
        if row["vehicle_tag"] == vehicle_tag and row["folder"]
    ]
    recorded = [
        {
            "vehicle_tag": row["vehicle_tag"], "drive_tag": row["drive_tag"],
            "view": row["view"], "status": row["status"],
            "reason_code": row["reason_code"], "reason": row["reason"],
            "clip_pairs": row["clip_pairs"],
        }
        for row in ledger.drives_with_status("parking_only", "skipped_short", "needs_review")
        if row["vehicle_tag"] == vehicle_tag and row["batch_id"] is None
    ]
    if not ready and not recorded:
        return {}

    folders = [Path(row["folder"]) for row in ready if Path(row["folder"]).is_dir()]
    built = delivery.build(
        folders,
        vehicle_tag=vehicle_tag,
        source_kind=(acquired.source_kind if acquired else "folder"),
        source_key=(acquired.source_key if acquired else None),
        recorded_items=recorded,
        pc_summary={
            "clips_on_source": acquired.clips_on_source if acquired else None,
            "detected_at": acquired.detected_at if acquired else None,
            "acquired_at": acquired.finished_at if acquired else None,
        },
    )
    ledger.record_batch(
        built.batch_id, vehicle_tag=vehicle_tag,
        source_kind=built.batch["source_kind"], source_key=built.batch["source_key"],
        drives=len(folders), clips_new=built.batch["clips_new"], bytes_new=built.bytes_total,
    )
    for row in ready:
        ledger.set_drive_status(row["vehicle_tag"], row["drive_tag"], "processed",
                                view=row["view"], batch_id=built.batch_id)
    for item in recorded:
        ledger.set_drive_status(item["vehicle_tag"], item["drive_tag"], item["status"],
                                view=item["view"], batch_id=built.batch_id)

    log(f"{vehicle_tag}: delivering {len(folders)} drive(s) as {built.batch_id[:8]}")
    try:
        reply = delivery.send(built, host=cfg.ssh_target, identity=cfg.server_identity)
    except delivery.DeliveryError as exc:
        attempts = ledger.bump_batch_attempt(built.batch_id, str(exc))
        log(f"{vehicle_tag}: could not deliver ({exc}); will try again (attempt {attempts})")
        return {"batch_id": built.batch_id, "delivered": False}

    ledger.update_batch(built.batch_id, shipped_at=utcnow(), server_status=reply[:200])
    for row in ready:
        ledger.set_drive_status(row["vehicle_tag"], row["drive_tag"], "shipped",
                                view=row["view"], batch_id=built.batch_id)
    log(f"{vehicle_tag}: server said {reply}")
    return {"batch_id": built.batch_id, "delivered": True}


def confirm_deliveries(
    cfg: Config,
    ledger: Ledger,
    *,
    log: Callable[[str], None] = print,
    card_stems: Optional[CardStems] = None,
) -> int:
    """Ask the server what became of anything delivered but not yet confirmed.

    With no copy of the video anywhere, this is also the moment the card can be
    declared safe to erase: once the server has what a delivery carried, nothing
    on the card it came from depends on it any more. ``card_stems`` says what is
    on that card right now, if it is still in the reader; the answer travels in
    the same message that reports the load.
    """
    confirmed = 0
    for batch in ledger.unconfirmed_batches():
        if not batch["shipped_at"]:
            continue
        result = delivery.import_result(cfg.api_base, batch["batch_id"])
        if not delivery.is_final(result):
            continue

        vehicle_tag = str(batch["vehicle_tag"])
        outcomes = delivery.outcome_by_drive(result)
        # Counted apart: a drive that loaded, one that was deliberately not
        # loaded (too short, or parked), one wanting a human look, and a real
        # failure. Lumping the middle two in with failures is what made a
        # perfectly good delivery announce itself as "0 drives loaded".
        loaded = failed = recorded = for_review = 0
        for (drive_tag, view), item in outcomes.items():
            status = str(item.get("status"))
            mapped = {
                "ingested": "ingested",
                "needs_review": "needs_review",
                "skipped_short": "skipped_short",
                "parking_only": "parking_only",
            }.get(status, "failed")
            ledger.set_drive_status(
                vehicle_tag, drive_tag, mapped, view=view,
                drive_session_id=item.get("drive_session_id"),
                reason_code=item.get("reason_code"), reason=item.get("reason"),
            )
            loaded += 1 if mapped == "ingested" else 0
            failed += 1 if mapped == "failed" else 0
            recorded += 1 if mapped in ("skipped_short", "parking_only") else 0
            for_review += 1 if mapped == "needs_review" else 0

        card_safe: Optional[bool] = None
        if card_stems is not None and batch["source_kind"] == "sdcard" and batch["source_key"]:
            stems = card_stems(str(batch["source_key"]))
            if stems is not None:
                waiting = ledger.unsettled_stems(vehicle_tag, stems)
                card_safe = not waiting
                if waiting:
                    log(f"{vehicle_tag}: {len(waiting)} clip(s) on {batch['source_key']} "
                        f"still depend on the card")

        ledger.update_batch(batch["batch_id"], confirmed_at=utcnow(),
                            server_status=str(result.get("status")))
        notify.import_done(ledger, vehicle_tag, loaded, failed,
                           str(batch["batch_id"]), cfg.viewer_base,
                           recorded=recorded, for_review=for_review, card_safe=card_safe)
        log(f"{vehicle_tag}: {batch['batch_id'][:8]} {result.get('status')} "
            f"({loaded} loaded, {recorded} recorded, {for_review} for review, {failed} failed)"
            + ("; card safe to erase" if card_safe else ""))
        confirmed += 1
    return confirmed


def tidy_holding(cfg: Config, ledger: Ledger, *, vehicle_tag: Optional[str] = None,
                 log: Callable[[str], None] = print) -> None:
    """Look after the video left over from the copy-first days.

    Nothing new is copied any more, but clips staged or held before that change
    are still here: anything settled is filed into holding, and holding is kept
    within its age and size limits until it has drained.
    """
    settled = {"ingested", "skipped_short", "parking_only", "failed"}
    movable: list[str] = []
    for row in ledger.drives_with_status(*settled):
        if vehicle_tag and row["vehicle_tag"] != vehicle_tag:
            continue
        for stem in ledger.drive_stems(row["vehicle_tag"], row["drive_tag"]):
            clip = ledger.get_clip(row["vehicle_tag"], stem)
            if clip is not None and clip["state"] == "staged":
                movable.append(stem)
    if movable and vehicle_tag:
        retain.move_to_holding(cfg, ledger, vehicle_tag, sorted(set(movable)), log=log)

    report = retain.prune(cfg, ledger, dry_run=False, log=log)
    if retain.should_warn(cfg, report):
        notify.holding_full(
            ledger,
            report.held_bytes_after / (1024 ** 3),
            cfg.holding_max_gb,
            retain.free_gb(cfg.holding_path),
        )


# --------------------------------------------------------------------------
# Entry points
# --------------------------------------------------------------------------

def run_once(cfg: Config, ledger: Ledger, *, log: Callable[[str], None] = print) -> list[PassResult]:
    """Deal with every card currently in a reader."""
    subpath = cfg.sources[0].subpath if cfg.sources else r"BlackVue\Record"
    found = sources.find_card_sources(subpath, cfg.exclude_volumes)
    if not found:
        # Still worth confirming anything outstanding and sending queued messages.
        confirm_deliveries(cfg, ledger, log=log)
        notify.flush(cfg, ledger, log=log)
        return []

    results = []
    with KeepAwake():
        for source in found:
            results.append(run_source(cfg, ledger, source, log=log))
    return results


def run_folder(cfg: Config, ledger: Ledger, folder: Path, vehicle_tag: Optional[str] = None,
               *, log: Callable[[str], None] = print) -> PassResult:
    """Treat a folder of clips as though it were a card."""
    source = FolderSource(folder, settle_seconds=0)
    vehicle = cfg.vehicle_by_tag(vehicle_tag) if vehicle_tag else None
    with KeepAwake():
        return run_source(cfg, ledger, source, vehicle=vehicle, log=log)


def watch(cfg: Config, ledger: Ledger, *, poll_seconds: float = 5.0,
          log: Callable[[str], None] = print) -> int:
    """Wait for a card, deal with it, and go back to waiting.

    A card is only read once it has been seen twice in a row, so a volume that is
    still mounting is never scanned half-ready.
    """
    subpath = cfg.sources[0].subpath if cfg.sources else r"BlackVue\Record"
    log(f"watching for a camera card every {poll_seconds:.0f}s (press Ctrl+C to stop)")

    seen: dict[str, int] = {}
    handled: set[str] = set()
    last_housekeeping = 0.0

    while True:
        try:
            present = {s.key: s for s in sources.find_card_sources(subpath, cfg.exclude_volumes)}

            for key in list(handled):
                if key not in present:
                    handled.discard(key)   # card taken out; ready for next time
                    seen.pop(key, None)
                    log(f"{key}: card removed")

            for key, source in present.items():
                seen[key] = seen.get(key, 0) + 1
                if key in handled or seen[key] < CONFIRM_POLLS:
                    continue
                log(f"{key}: card detected")
                with KeepAwake():
                    result = run_source(cfg, ledger, source, log=log)
                log(f"{key}: {result.summary}")
                if result.interrupted:
                    # It is already gone; let it be noticed afresh when it returns.
                    seen.pop(key, None)
                else:
                    handled.add(key)

            # Finishing off what was already sent does not depend on the reader
            # being empty. A delivery is confirmed a moment after it is sent, and
            # the server is often still loading it then, so the answer has to be
            # asked for again later -- and it is that answer that says whether
            # the card still in the reader can be erased.
            now = time.monotonic()
            if now - last_housekeeping >= HOUSEKEEPING_SECONDS:
                last_housekeeping = now
                if confirm_deliveries(cfg, ledger, log=log, card_stems=card_stems_for(present)):
                    for tag in sorted({v.tag for v in cfg.vehicles}):
                        tidy_holding(cfg, ledger, vehicle_tag=tag, log=log)
                notify.flush(cfg, ledger, log=log)

            time.sleep(poll_seconds)
        except KeyboardInterrupt:
            log("stopped")
            return 0
