"""The pipeline's command line.

    python -m dashcam_pipeline doctor          # is everything in place?
    python -m dashcam_pipeline seed-ledger     # learn what already exists
    python -m dashcam_pipeline watch           # wait for a card (the usual mode)
    python -m dashcam_pipeline run --folder D  # treat a folder as a card
    python -m dashcam_pipeline status          # what is where
    python -m dashcam_pipeline retry           # push anything stuck along
    python -m dashcam_pipeline retention       # prune held video (dry run by default)

Run `watch` with pythonw.exe from Task Scheduler and it sits quietly in the
background; everything it does goes to the log folder and the ledger.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import delivery, notify, retain, runner, seed as seed_mod
from .config import Config, ConfigError, check, load
from .ledger import Ledger


def log_to(path: Optional[Path]):
    """Print, and also append to a log file when one is wanted."""
    if path is None:
        return print
    path.parent.mkdir(parents=True, exist_ok=True)

    def _log(message: str) -> None:
        stamped = f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {message}"
        print(stamped, flush=True)
        try:
            with path.open("a", encoding="utf-8", errors="replace") as handle:
                handle.write(stamped + "\n")
        except OSError:
            pass

    return _log


def open_config(args: argparse.Namespace) -> Config:
    cfg = load(args.config)
    cfg.ensure_dirs()
    return cfg


# --------------------------------------------------------------------------
# Verbs
# --------------------------------------------------------------------------

def cmd_doctor(args: argparse.Namespace) -> int:
    """Everything that would stop this working, in one list."""
    try:
        cfg = open_config(args)
    except ConfigError as exc:
        print(f"configuration: {exc}")
        return 1

    print(f"configuration: {cfg.path}")
    problems = check(cfg)

    ledger_ok = True
    try:
        with Ledger(cfg.ledger_path) as led:
            counts = led.counts()
        print(f"ledger:        {cfg.ledger_path} (clips {counts['clips']}, drives {counts['drives']})")
    except Exception as exc:  # a broken ledger is a problem worth reporting, not a crash
        ledger_ok = False
        problems.append(f"the ledger at {cfg.ledger_path} could not be opened: {exc}")

    reachable = delivery.ping(cfg.ssh_target, cfg.server_identity)
    print(f"server:        {cfg.ssh_target} -> {'reachable' if reachable else 'NOT reachable'}")
    if not reachable:
        problems.append("the server did not answer; deliveries would be queued")

    result = delivery.import_result(cfg.api_base, "00000000-0000-0000-0000-000000000000")
    api_ok = result is not None or bool(cfg.api_base)
    print(f"database api:  {cfg.api_base or '(not configured)'}")

    print(f"holding:       {cfg.holding_path} "
          f"({retain.held_size_bytes(cfg) / (1024 ** 3):.0f} GB held, "
          f"{retain.free_gb(cfg.holding_path):.0f} GB free)")
    print(f"cameras:       " + ", ".join(f"{v.tag} ({v.serial})" for v in cfg.vehicles))

    if problems:
        print("\nproblems:")
        for p in problems:
            print(f"  - {p}")
        return 1
    print("\nnothing wrong found")
    return 0 if ledger_ok else 1


def cmd_seed(args: argparse.Namespace) -> int:
    cfg = open_config(args)
    log = log_to(cfg.log_dir / "pipeline.log")
    with Ledger(cfg.ledger_path) as led:
        report = seed_mod.seed(cfg, led, log=log)
    return 0 if not report.errors else 1


def cmd_watch(args: argparse.Namespace) -> int:
    cfg = open_config(args)
    log = log_to(cfg.log_dir / "pipeline.log")

    holder = runner.single_instance()
    if holder is None:
        log("another copy of the pipeline is already running; stopping")
        return 0

    with Ledger(cfg.ledger_path) as led:
        return runner.watch(cfg, led, poll_seconds=args.poll_seconds, log=log)


def cmd_run(args: argparse.Namespace) -> int:
    cfg = open_config(args)
    log = log_to(cfg.log_dir / "pipeline.log")

    holder = runner.single_instance()
    if holder is None:
        log("another copy of the pipeline is already running; stopping")
        return 0

    with Ledger(cfg.ledger_path) as led:
        if args.folder:
            result = runner.run_folder(cfg, led, Path(args.folder), args.vehicle, log=log)
            log(result.summary)
            return 1 if result.error else 0
        results = runner.run_once(cfg, led, log=log)
        if not results:
            log("no card found")
        for result in results:
            log(f"{result.source_key}: {result.summary}")
        return 0


def cmd_status(args: argparse.Namespace) -> int:
    cfg = open_config(args)
    with Ledger(cfg.ledger_path) as led:
        counts = led.counts()
        print("clips: " + (", ".join(f"{k} {v}" for k, v in sorted(counts["clips"].items())) or "none"))
        print("drives: " + (", ".join(f"{k} {v}" for k, v in sorted(counts["drives"].items())) or "none"))

        waiting = led.drives_with_status("planned", "processed", "shipped", "needs_review", "failed")
        if waiting:
            print("\nnot finished:")
            for row in waiting:
                reason = f" -- {row['reason']}" if row["reason"] else ""
                print(f"  {row['vehicle_tag']:6s} {row['drive_tag']:26s} {row['status']}{reason}")

        batches = led.unconfirmed_batches()
        if batches:
            print("\ndeliveries not yet confirmed:")
            for b in batches:
                state = "sent" if b["shipped_at"] else "not sent"
                print(f"  {b['batch_id'][:8]} {b['vehicle_tag'] or '?':6s} {state}"
                      f" (attempts {b['attempts']})"
                      + (f" -- {b['last_error']}" if b["last_error"] else ""))

        pending = led.pending_messages(limit=50)
        if pending:
            print(f"\n{len(pending)} notification(s) still to send")
    return 0


def cmd_retry(args: argparse.Namespace) -> int:
    """Push anything stuck: deliver what is built, confirm what was sent, send messages."""
    cfg = open_config(args)
    log = log_to(cfg.log_dir / "pipeline.log")
    with Ledger(cfg.ledger_path) as led:
        for vehicle in {v.tag for v in cfg.vehicles}:
            runner.deliver_pending(cfg, led, vehicle, log=log)
        runner.confirm_deliveries(cfg, led, log=log)
        result = notify.flush(cfg, led, log=log)
        log(f"notifications: {result['sent']} sent, {result['failed']} still waiting")
    return 0


def cmd_retention(args: argparse.Namespace) -> int:
    cfg = open_config(args)
    log = log_to(cfg.log_dir / "pipeline.log")
    with Ledger(cfg.ledger_path) as led:
        report = retain.prune(cfg, led, dry_run=not args.confirm, log=log)
    if report.dry_run:
        print("\nthis was a dry run; pass --confirm to actually remove them")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="dashcam_pipeline",
                                 description="Watch for a dashcam card and get it into the database.")
    ap.add_argument("--config", default=None, help="Configuration file (default: the usual place).")
    sub = ap.add_subparsers(dest="verb", required=True)

    sub.add_parser("doctor", help="Check everything is in place.").set_defaults(func=cmd_doctor)
    sub.add_parser("seed-ledger", help="Learn which drives already exist.").set_defaults(func=cmd_seed)
    sub.add_parser("status", help="Show what is where.").set_defaults(func=cmd_status)
    sub.add_parser("retry", help="Push anything stuck along.").set_defaults(func=cmd_retry)

    watch_p = sub.add_parser("watch", help="Wait for a card and deal with it.")
    watch_p.add_argument("--poll-seconds", type=float, default=5.0)
    watch_p.set_defaults(func=cmd_watch)

    run_p = sub.add_parser("run", help="Deal with a card, or a folder, once.")
    run_p.add_argument("--folder", default=None, help="Treat this folder of clips as a card.")
    run_p.add_argument("--vehicle", default=None, help="Which car, when a folder cannot say.")
    run_p.set_defaults(func=cmd_run)

    ret_p = sub.add_parser("retention", help="Prune held video.")
    ret_p.add_argument("--confirm", action="store_true", help="Actually remove them.")
    ret_p.set_defaults(func=cmd_retention)

    args = ap.parse_args(argv)
    try:
        return int(args.func(args))
    except ConfigError as exc:
        print(f"configuration: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
