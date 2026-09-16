"""One pass of the whole pipeline, and the loop that keeps taking passes.

A pass is the same whether it was started by a card appearing or by hand:

    identify the car -> copy what is new -> work out which drives those clips
    belong to -> build the ones worth building -> deliver them -> ask the server
    what became of them -> move the video into holding and prune it -> send the
    notifications that piled up along the way

Every step records what it did in the ledger before moving on, so an interrupted
pass loses nothing: the next one picks up from whatever is recorded.
"""
from __future__ import annotations

import ctypes
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional, Sequence

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

#: How often to ask the server what became of what we sent, file video whose
#: drive has landed, and send anything still queued for the phone. This happens
#: whether or not a card is in the reader: a card left in overnight must not stop
#: the pipeline finishing the work it has already done.
HOUSEKEEPING_SECONDS = 60.0


@dataclass
class PassResult:
    vehicle_tag: str = ""
    source_key: str = ""
    acquired: Optional[AcquireResult] = None
    plans: list[DrivePlan] = field(default_factory=list)
    built: list[process.ProcessResult] = field(default_factory=list)
    batch_id: Optional[str] = None
    delivered: bool = False
    error: Optional[str] = None

    @property
    def summary(self) -> str:
        bits = []
        if self.acquired:
            bits.append(f"{self.acquired.clips_copied} clip(s) copied")
        if self.plans:
            bits.append(plan.summarise(self.plans))
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
    """Stop Windows sleeping in the middle of copying 200 GB off a card."""

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

def _clips_to_consider(ledger: Ledger, vehicle_tag: str, new_stems: Sequence[str]) -> list[str]:
    """The new clips, plus any we already hold from around the same time.

    A card copied part-way through last time leaves a drive split across two
    visits; regrouping with the neighbours is what lets the planner see it as one
    drive and rebuild it under the identity it already has.
    """
    if not new_stems:
        return []
    window = timedelta(hours=6)
    stems = set(new_stems)
    for stem in list(new_stems):
        try:
            moment = datetime.strptime(stem[:15], "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        rows = ledger.clips_near(
            vehicle_tag,
            (moment - window).strftime("%Y%m%d_%H%M%S"),
            (moment + window).strftime("%Y%m%d_%H%M%S"),
        )
        stems.update(r["stem"] for r in rows if r["state"] != "purged")
    return sorted(stems)


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

    acquired = acquire.acquire_new_clips(cfg, ledger, source, vehicle, log=log)
    result.acquired = acquired
    acquire.announce(ledger, acquired)

    # Plan over everything nearby, not only what arrived just now.
    stems = _clips_to_consider(ledger, vehicle.tag, acquire.staged_clips(ledger, vehicle.tag))
    plans = plan.plan_drives(
        ledger, vehicle.tag, stems,
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

    built: list[process.ProcessResult] = []
    for drive in plans:
        if not drive.should_build:
            continue
        paths = acquire.staged_paths(ledger, vehicle.tag, drive.stems)
        outcome = process.process_drive(
            cfg, vehicle, drive, [paths[s] for s in drive.stems if s in paths], log=log
        )
        built.append(outcome)
        if outcome.ok:
            ledger.set_drive_status(
                vehicle.tag, drive.drive_tag, "processed", view=drive.view,
                folder=str(outcome.folder) if outcome.folder else None,
                drive_session_id=outcome.drive_session_id,
            )
        else:
            ledger.set_drive_status(
                vehicle.tag, drive.drive_tag, "failed", view=drive.view,
                reason_code=outcome.reason_code, reason=outcome.reason,
            )
            notify.processing_failed(ledger, vehicle.tag, drive.drive_tag, outcome.reason or "")
    result.built = built

    deliver_outcome = deliver_pending(cfg, ledger, vehicle.tag, acquired=acquired, log=log)
    result.batch_id = deliver_outcome.get("batch_id")
    result.delivered = bool(deliver_outcome.get("delivered"))

    confirm_deliveries(cfg, ledger, log=log)
    tidy_holding(cfg, ledger, vehicle.tag, log=log)
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


def confirm_deliveries(cfg: Config, ledger: Ledger, *, log: Callable[[str], None] = print) -> int:
    """Ask the server what became of anything delivered but not yet confirmed."""
    confirmed = 0
    for batch in ledger.unconfirmed_batches():
        if not batch["shipped_at"]:
            continue
        result = delivery.import_result(cfg.api_base, batch["batch_id"])
        if not delivery.is_final(result):
            continue

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
                str(batch["vehicle_tag"]), drive_tag, mapped, view=view,
                drive_session_id=item.get("drive_session_id"),
                reason_code=item.get("reason_code"), reason=item.get("reason"),
            )
            loaded += 1 if mapped == "ingested" else 0
            failed += 1 if mapped == "failed" else 0
            recorded += 1 if mapped in ("skipped_short", "parking_only") else 0
            for_review += 1 if mapped == "needs_review" else 0

        ledger.update_batch(batch["batch_id"], confirmed_at=utcnow(),
                            server_status=str(result.get("status")))
        notify.import_done(ledger, str(batch["vehicle_tag"]), loaded, failed,
                           str(batch["batch_id"]), cfg.viewer_base,
                           recorded=recorded, for_review=for_review)
        log(f"{batch['vehicle_tag']}: {batch['batch_id'][:8]} {result.get('status')} "
            f"({loaded} loaded, {recorded} recorded, {for_review} for review, {failed} failed)")
        confirmed += 1
    return confirmed


def tidy_holding(cfg: Config, ledger: Ledger, vehicle_tag: str,
                 *, log: Callable[[str], None] = print) -> None:
    """Move what has been dealt with into holding, and keep holding in bounds."""
    settled = {"ingested", "skipped_short", "parking_only", "failed"}
    movable: list[str] = []
    for row in ledger.drives_with_status(*settled):
        if row["vehicle_tag"] != vehicle_tag:
            continue
        movable.extend(ledger.drive_stems(vehicle_tag, row["drive_tag"]))
    if movable:
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
                handled.add(key)

            # Finishing off what was already sent does not depend on the reader
            # being empty. A delivery is confirmed a moment after it is sent, and
            # the server is often still loading it then, so the answer has to be
            # asked for again later -- and the video of a loaded drive cannot be
            # filed into holding until that answer arrives. Gating this on an
            # empty reader meant a card left in the machine stopped both for as
            # long as it sat there.
            now = time.monotonic()
            if now - last_housekeeping >= HOUSEKEEPING_SECONDS:
                last_housekeeping = now
                if confirm_deliveries(cfg, ledger, log=log):
                    for tag in sorted({v.tag for v in cfg.vehicles}):
                        tidy_holding(cfg, ledger, tag, log=log)
                notify.flush(cfg, ledger, log=log)

            time.sleep(poll_seconds)
        except KeyboardInterrupt:
            log("stopped")
            return 0
