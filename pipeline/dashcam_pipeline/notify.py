"""Telling you what happened.

Messages are written to the ledger's outbox first and sent afterwards, so a
notification is never lost because the network was down, the server was asleep,
or the phone was out of reach. Anything unsent is retried on the next pass.

Sending is always best-effort: a notification that cannot be delivered must never
stop a card being copied or a drive being loaded.
"""
from __future__ import annotations

import urllib.error
import urllib.request
from typing import Optional

from .config import Config
from .ledger import Ledger

#: Give up on a message after this many tries rather than retrying for ever.
MAX_ATTEMPTS = 8

#: Kinds of message, so the pipeline's own code reads clearly at the call site.
CARD_COPIED = "card_copied"
SAFE_TO_ERASE = "safe_to_erase"
NOTHING_NEW = "nothing_new"
CARD_REMOVED = "card_removed"
UNKNOWN_CAMERA = "unknown_camera"
PROCESSING_FAILED = "processing_failed"
NEEDS_REVIEW = "needs_review"
HOLDING_FULL = "holding_full"
IMPORT_DONE = "import_done"


def queue(ledger: Ledger, kind: str, title: str, message: str,
          *, tags: str = "", priority: str = "") -> int:
    """Record something worth telling the owner about."""
    return ledger.queue_message(kind, title, message, tags=tags, priority=priority)


def _post(cfg: Config, title: str, message: str, tags: str, priority: str) -> None:
    request = urllib.request.Request(
        f"{cfg.ntfy_url}/{cfg.ntfy_topic}",
        data=message.encode("utf-8"),
        method="POST",
    )
    request.add_header("Title", title)
    if tags:
        request.add_header("Tags", tags)
    if priority:
        request.add_header("Priority", priority)
    if cfg.ntfy_token:
        request.add_header("Authorization", f"Bearer {cfg.ntfy_token}")
    with urllib.request.urlopen(request, timeout=10):
        pass


def flush(cfg: Config, ledger: Ledger, *, log=print) -> dict[str, int]:
    """Send whatever is waiting. Returns how it went, and never raises."""
    if not cfg.ntfy_url or not cfg.ntfy_topic:
        return {"sent": 0, "failed": 0, "skipped": 0, "given_up": 0}

    sent = failed = given_up = 0
    for row in ledger.pending_messages():
        if int(row["attempts"] or 0) >= MAX_ATTEMPTS:
            # Stop retrying, but leave it in the outbox so `status` can show it.
            given_up += 1
            continue
        try:
            _post(cfg, row["title"], row["message"], row["tags"] or "", row["priority"] or "")
        except (urllib.error.URLError, OSError, ValueError) as exc:
            ledger.mark_message_failed(int(row["id"]), f"{type(exc).__name__}: {exc}")
            failed += 1
            log(f"notification not sent (will retry): {exc}")
            continue
        ledger.mark_message_sent(int(row["id"]))
        sent += 1

    return {"sent": sent, "failed": failed, "skipped": 0, "given_up": given_up}


def send_now(cfg: Config, ledger: Ledger, kind: str, title: str, message: str,
             *, tags: str = "", priority: str = "", log=print) -> bool:
    """Queue a message and try to send it immediately.

    Used for the things worth knowing at once -- the card is safe to erase, a
    drive needs a look -- while still leaving a record if the send fails.
    """
    queue(ledger, kind, title, message, tags=tags, priority=priority)
    result = flush(cfg, ledger, log=log)
    return result["sent"] > 0


# --------------------------------------------------------------------------
# The messages themselves, in one place so the wording stays consistent
# --------------------------------------------------------------------------

def card_copied(ledger: Ledger, vehicle: str, clips: int, gigabytes: float) -> None:
    queue(
        ledger, CARD_COPIED,
        f"{vehicle}: card copied",
        f"{clips} new clip{'s' if clips != 1 else ''} copied ({gigabytes:.1f} GB). Processing now.",
        tags="inbox_tray",
    )


def safe_to_erase(ledger: Ledger, vehicle: str, clips: int) -> None:
    queue(
        ledger, SAFE_TO_ERASE,
        f"{vehicle}: safe to erase the card",
        f"All {clips} clip{'s' if clips != 1 else ''} on the card are copied and verified. "
        f"The card can be formatted in the camera whenever you like.",
        tags="white_check_mark",
    )


def nothing_new(ledger: Ledger, vehicle: str) -> None:
    queue(
        ledger, NOTHING_NEW,
        f"{vehicle}: nothing new",
        "Everything on the card is already here. Safe to erase.",
        tags="white_check_mark",
    )


def card_removed(ledger: Ledger, vehicle: str, copied: int, remaining: int) -> None:
    queue(
        ledger, CARD_REMOVED,
        f"{vehicle}: card removed while copying",
        f"{copied} clip{'s' if copied != 1 else ''} copied, {remaining} still to go. "
        f"Put the card back in and it will carry on where it stopped.",
        tags="warning", priority="high",
    )


def unknown_camera(ledger: Ledger, where: str, serial: Optional[str]) -> None:
    queue(
        ledger, UNKNOWN_CAMERA,
        "Unknown camera",
        f"A card in {where} holds footage from camera {serial or 'an unreadable serial'}, "
        f"which is not one of the configured cars. Nothing was copied.",
        tags="question", priority="high",
    )


def processing_failed(ledger: Ledger, vehicle: str, drive_tag: str, reason: str) -> None:
    queue(
        ledger, PROCESSING_FAILED,
        f"{vehicle}: a drive could not be processed",
        f"{drive_tag}: {reason}",
        tags="x", priority="high",
    )


def needs_review(ledger: Ledger, vehicle: str, drive_tag: str, reason: str) -> None:
    queue(
        ledger, NEEDS_REVIEW,
        f"{vehicle}: a drive needs a look",
        f"{drive_tag}: {reason}",
        tags="mag", priority="high",
    )


def holding_full(ledger: Ledger, used_gb: float, cap_gb: float, free_gb: float) -> None:
    queue(
        ledger, HOLDING_FULL,
        "Video holding is at its limit",
        f"{used_gb:.0f} GB held of a {cap_gb:.0f} GB limit, {free_gb:.0f} GB free on the disk. "
        f"The oldest clips are being removed first; nothing belonging to a drive that has not "
        f"reached the database is touched.",
        tags="warning", priority="high",
    )


def import_done(ledger: Ledger, vehicle: str, loaded: int, failed: int,
                batch_id: str, viewer_base: str = "") -> None:
    body = f"{loaded} drive{'s' if loaded != 1 else ''} loaded"
    if failed:
        body += f", {failed} did not load"
    if viewer_base:
        body += f"\n{viewer_base}/imports.html?batch={batch_id}"
    queue(
        ledger, IMPORT_DONE,
        f"{vehicle}: {'loaded' if not failed else 'loaded with problems'}",
        body,
        tags="white_check_mark" if not failed else "warning",
        priority="default" if not failed else "high",
    )
