"""Turning a group of clips into a drive folder.

One subprocess per drive, running pipeline/blackvue_drives.py with the tag and
the identifier the pipeline has already decided on. That is the important part:
the script is told which drive this is rather than working it out for itself, so
a drive that is processed again lands on the same identity and updates the drive
already in the database instead of becoming a second copy of it.

Its exit code is the contract:

    0   built
    3   nothing to do -- already built with the same clips and the same options
    4   built, but with no accelerometer data, which the database loader requires
    anything else  failure
"""
from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional, Sequence

from .config import Config, Vehicle
from .plan import DrivePlan

#: The drives script lives beside this package.
DRIVES_SCRIPT = Path(__file__).resolve().parents[1] / "blackvue_drives.py"

#: Written last inside a finished drive folder.
COMPLETE_MARKER = "_complete.json"

EXIT_BUILT = 0
EXIT_NOTHING_TO_DO = 3
EXIT_NO_ACCEL = 4


@dataclass
class ProcessResult:
    drive_tag: str
    vehicle_tag: str
    view: str
    status: str                    # processed | skipped | failed
    folder: Optional[Path] = None
    drive_session_id: Optional[str] = None
    exit_code: Optional[int] = None
    reason_code: Optional[str] = None
    reason: Optional[str] = None
    log_path: Optional[Path] = None
    duration_s: float = 0.0

    @property
    def ok(self) -> bool:
        return self.status in ("processed", "skipped")


def drive_session_id_for(vehicle_tag: str, drive_tag: str) -> str:
    """The identifier the drives script and the database will both derive."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"blackvue-drive:{vehicle_tag}:{drive_tag}"))


def write_clip_list(path: Path, clip_paths: Sequence[Path]) -> Path:
    """The file the drives script reads instead of scanning a folder."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(str(Path(p)) for p in clip_paths) + "\n",
        encoding="utf-8",  # no byte-order mark: the script strips one, but do not add it
    )
    return path


def build_command(
    cfg: Config, vehicle: Vehicle, drive: DrivePlan, clip_list: Path, batch_id: Optional[str]
) -> list[str]:
    """The drives-script command line for one drive."""
    p = cfg.processing
    python = cfg.tools.get("python") or "python"

    cmd = [
        python, str(DRIVES_SCRIPT),
        "--clips-from", str(clip_list),
        "--drive-tag", drive.drive_tag,
        "--drive-session-id", drive_session_id_for(vehicle.tag, drive.drive_tag),
        "--vehicle", vehicle.tag,
        "--output", str(vehicle.output_root),
        "--output-layout", "import",
        "--camera-utc-offset", cfg.camera_utc_offset,
        "--timezone", str(p.get("timezone") or "America/Chicago"),
        "--clip-seconds", str(p.get("clip_seconds") or 60),
        "--gap-seconds", str(p.get("gap_seconds") or 120),
        "--max-failed-clip-ratio", str(p.get("max_failed_clip_ratio") or 0.2),
        "--run-blackclue",
        "--blackclue", cfg.tools.get("blackclue") or "blackclue",
        "--blackclue-mode", str(p.get("blackclue_mode") or "complete"),
        "--blackclue-view", str(p.get("blackclue_view") or "front"),
        "--blackclue-jobs", str(p.get("blackclue_jobs") or 4),
        "--telemetry-sidecar-action", "move",
        "--ffmpeg", cfg.tools.get("ffmpeg") or "ffmpeg",
        "--ffmpeg-hwaccel", str(p.get("ffmpeg_hwaccel") or "auto"),
        "--emit-csv", "--emit-gpx", "--emit-accel",
        "--emit-stills",
        "--stills-every-seconds", str(p.get("stills_every_seconds") or 30),
        "--stills-width", str(p.get("stills_width") or 1920),
        "--stills-format", str(p.get("stills_format") or "jpg"),
        "--stills-quality", str(p.get("stills_quality") or 3),
        "--stills-name", str(p.get("stills_name") or "utc"),
        "--stills-geojson",
        "--stills-view", str(p.get("blackclue_view") or "front"),
        "--emit-manifest",
        "--skip-existing",
    ]
    if cfg.tools.get("ffprobe"):
        cmd += ["--ffprobe", cfg.tools["ffprobe"]]
    if vehicle.model:
        cmd += ["--source-model", vehicle.model]
    if vehicle.firmware:
        cmd += ["--source-firmware", vehicle.firmware]
    if vehicle.serial:
        cmd += ["--source-serial", vehicle.serial]
    if batch_id:
        cmd += ["--batch-id", batch_id]
    return cmd


def expected_folder(vehicle: Vehicle, drive: DrivePlan) -> Path:
    return vehicle.output_root / vehicle.tag / f"{drive.drive_tag}_{drive.view}"


def process_drive(
    cfg: Config,
    vehicle: Vehicle,
    drive: DrivePlan,
    clip_paths: Sequence[Path],
    *,
    batch_id: Optional[str] = None,
    log: Callable[[str], None] = print,
    timeout: Optional[float] = None,
) -> ProcessResult:
    """Build one drive folder, and say plainly what happened."""
    started = datetime.now(timezone.utc)
    result = ProcessResult(
        drive_tag=drive.drive_tag, vehicle_tag=vehicle.tag, view=drive.view,
        status="failed",
        drive_session_id=drive_session_id_for(vehicle.tag, drive.drive_tag),
    )

    if not DRIVES_SCRIPT.is_file():
        result.reason_code = "NO_DRIVES_SCRIPT"
        result.reason = f"the drives script is not at {DRIVES_SCRIPT}"
        return result
    if not clip_paths:
        result.reason_code = "NO_CLIPS"
        result.reason = "no clip files were found for this drive"
        return result

    lists_dir = cfg.state_dir / "clip_lists"
    clip_list = write_clip_list(lists_dir / f"{drive.drive_tag}_{drive.view}.txt", clip_paths)

    log_dir = cfg.log_dir / "drives"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{drive.drive_tag}_{drive.view}.log"
    result.log_path = log_path

    cmd = build_command(cfg, vehicle, drive, clip_list, batch_id)
    log(f"{vehicle.tag}: building {drive.drive_tag} from {len(clip_paths)} clip file(s)")

    try:
        with log_path.open("w", encoding="utf-8", errors="replace") as handle:
            handle.write(" ".join(cmd) + "\n\n")
            handle.flush()
            done = subprocess.run(cmd, stdout=handle, stderr=subprocess.STDOUT, timeout=timeout)
        code = done.returncode
    except subprocess.TimeoutExpired:
        result.reason_code = "TIMED_OUT"
        result.reason = f"processing took longer than {timeout} seconds"
        return result
    except OSError as exc:
        result.reason_code = "COULD_NOT_RUN"
        result.reason = f"{type(exc).__name__}: {exc}"
        return result
    finally:
        result.duration_s = (datetime.now(timezone.utc) - started).total_seconds()

    result.exit_code = code
    folder = expected_folder(vehicle, drive)
    result.folder = folder if folder.is_dir() else None

    if code == EXIT_BUILT:
        result.status = "processed"
    elif code == EXIT_NOTHING_TO_DO:
        result.status = "skipped"
        result.reason = "already built from these clips with these options"
    elif code == EXIT_NO_ACCEL:
        result.status = "failed"
        result.reason_code = "NO_ACCEL_CSV"
        result.reason = ("built, but no accelerometer data came out of the clips, "
                         "and the database loader requires it")
    else:
        result.status = "failed"
        result.reason_code = "PROCESSING_FAILED"
        result.reason = f"the drives script exited {code}; see {log_path.name}"

    if result.status in ("processed", "skipped"):
        problem = check_folder(folder)
        if problem:
            result.status = "failed"
            result.reason_code = "INCOMPLETE_FOLDER"
            result.reason = problem

    log(f"{vehicle.tag}: {drive.drive_tag} -> {result.status}"
        + (f" ({result.reason})" if result.reason else "")
        + f" in {result.duration_s:.0f}s")
    return result


def check_folder(folder: Optional[Path]) -> Optional[str]:
    """Is this really a finished drive folder? Returns the problem, or None."""
    if folder is None or not folder.is_dir():
        return "the drive folder was not created"
    if not (folder / "manifest.json").is_file():
        return "the drive folder has no manifest"
    if not (folder / COMPLETE_MARKER).is_file():
        return "the drive folder has no completion marker, so it may be half-built"
    return None
