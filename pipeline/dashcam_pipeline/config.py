"""Reading and checking the pipeline's configuration.

Standard library only: this runs unattended from Task Scheduler, so it must not
depend on anything that could be missing or half-upgraded.

The file is TOML at %LOCALAPPDATA%\\dashcam-pipeline\\config.toml, with
pipeline/config.example.toml as the template. Paths in it may use %VAR% and ~.
"""
from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

DEFAULT_CONFIG_PATH = r"%LOCALAPPDATA%\dashcam-pipeline\config.toml"

#: Kept in step with pipeline/blackvue_drives.py.
DEFAULT_CAMERA_UTC_OFFSET = "-06:00"


class ConfigError(Exception):
    """The configuration is missing something, or says something impossible."""


def expand(value: str) -> Path:
    """Expand %VAR% and ~ and return an absolute path."""
    return Path(os.path.expandvars(str(value))).expanduser()


@dataclass(frozen=True)
class Vehicle:
    """One camera, identified by the serial embedded in its clips."""

    serial: str
    tag: str
    output_root: Path
    model: Optional[str] = None
    firmware: Optional[str] = None


@dataclass(frozen=True)
class Source:
    kind: str
    subpath: str = r"BlackVue\Record"


@dataclass
class Config:
    path: Path

    state_dir: Path
    staging_dir: Path
    trash_dir: Path
    keep_dirs: list[Path]
    exclude_volumes: list[str]

    tools: dict[str, str]
    processing: dict[str, Any]

    holding_path: Path
    holding_days: int
    holding_max_gb: float
    holding_min_free_gb: float

    server_host: str
    server_user: str
    server_identity: Optional[Path]
    api_base: str
    viewer_base: str

    ntfy_url: str
    ntfy_topic: str
    ntfy_token: Optional[str]

    vehicles: list[Vehicle] = field(default_factory=list)
    sources: list[Source] = field(default_factory=list)

    # --- derived helpers -------------------------------------------------

    @property
    def ledger_path(self) -> Path:
        return self.state_dir / "state.sqlite"

    @property
    def log_dir(self) -> Path:
        return self.state_dir / "logs"

    @property
    def camera_utc_offset(self) -> str:
        return str(self.processing.get("camera_utc_offset") or DEFAULT_CAMERA_UTC_OFFSET)

    @property
    def driving_types(self) -> set[str]:
        return {str(t).upper() for t in (self.processing.get("driving_types") or ["N", "E", "M", "I"])}

    def vehicle_by_serial(self, serial: Optional[str]) -> Optional[Vehicle]:
        if not serial:
            return None
        wanted = serial.strip().upper()
        for v in self.vehicles:
            if v.serial.strip().upper() == wanted:
                return v
        return None

    def vehicle_by_tag(self, tag: str) -> Optional[Vehicle]:
        for v in self.vehicles:
            if v.tag == tag:
                return v
        return None

    @property
    def ssh_target(self) -> str:
        return f"{self.server_user}@{self.server_host}"

    def ensure_dirs(self) -> None:
        """Create the folders the pipeline writes to."""
        for d in (self.state_dir, self.staging_dir, self.log_dir, self.holding_path):
            d.mkdir(parents=True, exist_ok=True)


def _require(table: dict[str, Any], key: str, where: str) -> Any:
    if key not in table:
        raise ConfigError(f"{where} is missing '{key}'")
    return table[key]


def load(path: Optional[str | Path] = None) -> Config:
    """Read the configuration, or explain clearly what is wrong with it."""
    config_path = expand(str(path or DEFAULT_CONFIG_PATH))
    if not config_path.is_file():
        raise ConfigError(
            f"no configuration at {config_path}. Copy pipeline/config.example.toml "
            f"there and edit it."
        )

    try:
        raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"{config_path} is not valid TOML: {exc}") from exc

    paths = raw.get("paths") or {}
    holding = raw.get("holding") or {}
    server = raw.get("server") or {}
    ntfy = raw.get("ntfy") or {}

    state_dir = expand(_require(paths, "state_dir", "[paths]"))
    staging_dir = expand(paths.get("staging_dir") or (state_dir / "staging"))

    vehicles = []
    for entry in raw.get("vehicles") or []:
        vehicles.append(
            Vehicle(
                serial=str(_require(entry, "serial", "[[vehicles]]")),
                tag=str(_require(entry, "tag", "[[vehicles]]")),
                output_root=expand(_require(entry, "output_root", "[[vehicles]]")),
                model=entry.get("model"),
                firmware=entry.get("firmware"),
            )
        )
    if not vehicles:
        raise ConfigError("no [[vehicles]] are configured, so no card could be identified")

    seen: set[str] = set()
    for v in vehicles:
        key = v.serial.strip().upper()
        if key in seen:
            raise ConfigError(f"two vehicles share the serial {v.serial}")
        seen.add(key)

    sources = [
        Source(kind=str(s.get("kind") or "sdcard"), subpath=str(s.get("subpath") or r"BlackVue\Record"))
        for s in (raw.get("sources") or [{"kind": "sdcard"}])
    ]

    token: Optional[str] = None
    token_file = ntfy.get("token_file")
    if token_file:
        token_path = expand(token_file)
        if token_path.is_file():
            token = token_path.read_text(encoding="utf-8").strip() or None

    identity = server.get("identity_file")

    cfg = Config(
        path=config_path,
        state_dir=state_dir,
        staging_dir=staging_dir,
        trash_dir=expand(paths.get("trash_dir") or (state_dir / "trash")),
        keep_dirs=[expand(p) for p in (paths.get("keep_dirs") or [])],
        exclude_volumes=[str(v).rstrip("\\").upper() for v in (paths.get("exclude_volumes") or [])],
        tools={k: str(v) for k, v in (raw.get("tools") or {}).items()},
        processing=dict(raw.get("processing") or {}),
        holding_path=expand(holding.get("path") or (state_dir / "holding")),
        holding_days=int(holding.get("days") or 14),
        holding_max_gb=float(holding.get("max_gb") or 800),
        holding_min_free_gb=float(holding.get("min_free_gb") or 100),
        server_host=str(server.get("host") or ""),
        server_user=str(server.get("user") or ""),
        server_identity=expand(identity) if identity else None,
        api_base=str(server.get("api_base") or "").rstrip("/"),
        viewer_base=str(server.get("viewer_base") or "").rstrip("/"),
        ntfy_url=str(ntfy.get("url") or "").rstrip("/"),
        ntfy_topic=str(ntfy.get("topic") or ""),
        ntfy_token=token,
        vehicles=vehicles,
        sources=sources,
    )
    return cfg


def check(cfg: Config) -> list[str]:
    """Everything that would stop the pipeline working, in plain words.

    Returned rather than raised: `doctor` shows the whole list at once instead of
    making someone fix one thing per run.
    """
    problems: list[str] = []

    for name in ("blackclue", "ffmpeg"):
        value = cfg.tools.get(name)
        if not value:
            problems.append(f"[tools] does not say where {name} is")
        elif os.sep in str(value) and not Path(os.path.expandvars(str(value))).is_file():
            problems.append(f"{name} is not at {value}")

    for v in cfg.vehicles:
        if not v.output_root.parent.exists():
            problems.append(f"{v.tag}: the folder above {v.output_root} does not exist")

    if not cfg.server_host or not cfg.server_user:
        problems.append("[server] needs both host and user to deliver anything")
    if cfg.server_identity and not cfg.server_identity.is_file():
        problems.append(f"the delivery key is not at {cfg.server_identity}")

    if cfg.ntfy_url and not cfg.ntfy_token:
        problems.append("[ntfy] has a url but no token was found, so nothing can be sent")

    if cfg.holding_max_gb <= 0:
        problems.append("[holding] max_gb must be greater than zero")
    if cfg.holding_days <= 0:
        problems.append("[holding] days must be greater than zero")

    excluded = set(cfg.exclude_volumes)
    for v in cfg.vehicles:
        drive = str(v.output_root.drive).rstrip("\\").upper()
        if drive and drive not in excluded:
            problems.append(
                f"{v.tag}: {drive} holds processed drives but is not in exclude_volumes, "
                f"so it could be mistaken for a camera card"
            )

    return problems
