"""Find re-processed duplicates among processed drive folders. Reports only.

The processing script used to add ``_02`` / ``_03`` to a drive's folder name
whenever a folder of that name already existed, so the same drive can sit on
disk two or three times over. This tool groups those folders back together,
measures how complete each copy is, and says which one is worth keeping.

It never deletes, moves or renames anything. The "move the rest to a trash
folder" line in the output is a recommendation for a human to act on.

Usage::

    python dedupe_folders.py \
        --root "F:\\Dashcam\\Processed_Camry\\camry" \
        --root "F:\\Dashcam\\Processed_Civic\\civic" \
        --report-dir .

"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

#: A drive folder looks like ``20260721_171555_civic_02_front``.
FOLDER_RE = re.compile(
    r"^(?P<stamp>\d{8}_\d{6})_(?P<vehicle>[A-Za-z0-9]+)"
    r"(?:_(?P<run>\d{2}))?"
    r"_(?P<view>front|rear|interior|other)$"
)

DEFAULT_TRASH_DIRNAME = "_trash_duplicates"

THUMB_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}


# --------------------------------------------------------------------------
# Measuring one folder
# --------------------------------------------------------------------------

def count_csv_rows(path: Path) -> Optional[int]:
    """Number of data rows in a CSV (total lines minus the header)."""
    if not path.is_file():
        return None
    lines = 0
    try:
        with path.open("rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                lines += chunk.count(b"\n")
        # A file not ending in a newline still has a final row.
        with path.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            if size:
                f.seek(-1, 2)
                if f.read(1) != b"\n":
                    lines += 1
    except OSError as exc:
        return None
    return max(lines - 1, 0)


def load_manifest(folder: Path) -> tuple[bool, Optional[dict[str, Any]], Optional[str]]:
    p = folder / "manifest.json"
    if not p.is_file():
        return False, None, None
    try:
        return True, json.loads(p.read_text(encoding="utf-8")), None
    except (OSError, ValueError) as exc:
        return True, None, f"{type(exc).__name__}: {exc}"


def count_thumbs(folder: Path) -> int:
    d = folder / "artifacts" / "thumbs"
    if not d.is_dir():
        return 0
    return sum(
        1 for p in d.iterdir()
        if p.is_file() and p.suffix.lower() in THUMB_SUFFIXES
    )


def dir_size_bytes(folder: Path) -> int:
    total = 0
    for p in folder.rglob("*"):
        try:
            if p.is_file():
                total += p.stat().st_size
        except OSError:
            continue
    return total


def inspect(folder: Path) -> dict[str, Any]:
    """Everything we know about one candidate folder."""
    name = folder.name
    m = FOLDER_RE.match(name)
    stamp = m.group("stamp") if m else None
    vehicle = m.group("vehicle") if m else None
    run = m.group("run") if m else None
    view = m.group("view") if m else None
    base_tag = f"{stamp}_{vehicle}" if m else None

    has_manifest, manifest, manifest_error = load_manifest(folder)

    clip_count = None
    manifest_session_id = None
    gnss_rel = accel_rel = None
    if isinstance(manifest, dict):
        clips = manifest.get("clips")
        clip_count = len(clips) if isinstance(clips, list) else None
        manifest_session_id = manifest.get("drive_session_id")
        telem = manifest.get("telemetry") or {}
        if isinstance(telem, dict):
            g = telem.get("gnss")
            a = telem.get("accel")
            gnss_rel = g.get("path") if isinstance(g, dict) else telem.get("gnss_csv")
            accel_rel = a.get("path") if isinstance(a, dict) else telem.get("accel_csv")

    # Fall back to the conventional names when the manifest is missing.
    gnss_path = folder / (gnss_rel or f"{name}.csv")
    accel_path = folder / (accel_rel or f"{name}_accel.csv")

    gnss_rows = count_csv_rows(gnss_path)
    accel_rows = count_csv_rows(accel_path)
    thumbs = count_thumbs(folder)

    return {
        "folder": name,
        "path": str(folder),
        "base_tag": base_tag,
        "stamp": stamp,
        "vehicle": vehicle,
        "run_suffix": run,
        "view": view,
        "name_parsed": bool(m),
        "has_manifest": has_manifest,
        "manifest_error": manifest_error,
        "manifest_drive_session_id": manifest_session_id,
        "clip_count": clip_count,
        "gnss_csv": gnss_path.name,
        "gnss_csv_exists": gnss_path.is_file(),
        "gnss_rows": gnss_rows,
        "accel_csv": accel_path.name,
        "accel_csv_exists": accel_path.is_file(),
        "accel_rows": accel_rows,
        "thumb_count": thumbs,
        "size_bytes": dir_size_bytes(folder),
    }


def completeness_key(c: dict[str, Any]) -> tuple:
    """Ordering key: the biggest tuple is the most complete candidate.

    A manifest matters most (without it the folder cannot be loaded at all),
    then the number of clips, then how much telemetry and how many preview
    images survived.

    When those all tie the copies hold the same drive, so the folder whose name
    has no ``_02`` style suffix wins: that is the name the drive's database
    identifier is derived from. Total size is only the last resort, because a
    few kilobytes of difference between otherwise identical copies says nothing
    about which one is more complete.
    """
    return (
        1 if c["has_manifest"] and not c["manifest_error"] else 0,
        c["clip_count"] or 0,
        c["gnss_rows"] or 0,
        c["accel_rows"] or 0,
        c["thumb_count"],
        1 if c["run_suffix"] is None else 0,
        c["size_bytes"],
    )


def describe_gap(keep: dict[str, Any], other: dict[str, Any]) -> str:
    """Plain words for why ``other`` loses to ``keep``."""
    bits = []
    if (keep["has_manifest"] and not keep["manifest_error"]) and not (
        other["has_manifest"] and not other["manifest_error"]
    ):
        bits.append("no usable manifest")
    for label, key in (("clips", "clip_count"), ("GPS rows", "gnss_rows"),
                       ("accel rows", "accel_rows"), ("preview images", "thumb_count")):
        k, o = keep.get(key) or 0, other.get(key) or 0
        if o < k:
            bits.append(f"{o} {label} vs {k}")
        elif o > k:
            bits.append(f"more {label} ({o} vs {k})")
    if not bits:
        bits.append("identical content, duplicate name")
    return "; ".join(bits)


# --------------------------------------------------------------------------
# Grouping
# --------------------------------------------------------------------------

def scan_roots(roots: Iterable[Path]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for root in roots:
        if not root.is_dir():
            skipped.append({"path": str(root), "reason": "root not found"})
            continue
        for folder in sorted(p for p in root.iterdir() if p.is_dir()):
            if folder.name == DEFAULT_TRASH_DIRNAME:
                continue
            info = inspect(folder)
            info["root"] = str(root)
            if info["name_parsed"]:
                candidates.append(info)
            else:
                skipped.append({"path": str(folder), "reason": "name does not match the drive folder pattern"})
    return candidates, skipped


def group_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group by base tag (and camera view, so front and rear never compete)."""
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for c in candidates:
        key = (c["root"], c["base_tag"], c["view"])
        groups.setdefault(key, []).append(c)

    out = []
    for (root, base_tag, view), members in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2])):
        ranked = sorted(members, key=completeness_key, reverse=True)
        keep = ranked[0]
        drop = ranked[1:]
        trash_dir = str(Path(root) / DEFAULT_TRASH_DIRNAME)
        out.append(
            {
                "root": root,
                "base_tag": base_tag,
                "view": view,
                "candidate_count": len(members),
                "is_duplicate_group": len(members) > 1,
                "keep": keep["folder"],
                "keep_reason": (
                    "only copy" if len(members) == 1 else "most complete copy"
                ),
                "recommend_move": [
                    {
                        "folder": d["folder"],
                        "from": d["path"],
                        "to": str(Path(trash_dir) / d["folder"]),
                        "why": describe_gap(keep, d),
                    }
                    for d in drop
                ],
                "candidates": ranked,
            }
        )
    return out


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------

def summarise(groups: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    dup_groups = [g for g in groups if g["is_duplicate_group"]]
    per_root: dict[str, dict[str, int]] = {}
    for c in candidates:
        d = per_root.setdefault(
            c["root"],
            {"folders": 0, "with_suffix": 0, "no_manifest": 0},
        )
        d["folders"] += 1
        if c["run_suffix"] is not None:
            d["with_suffix"] += 1
        if not c["has_manifest"] or c["manifest_error"]:
            d["no_manifest"] += 1
    for root, d in per_root.items():
        d["groups"] = sum(1 for g in groups if g["root"] == root)
        d["duplicate_groups"] = sum(1 for g in dup_groups if g["root"] == root)
        d["recommend_move"] = sum(
            len(g["recommend_move"]) for g in groups if g["root"] == root
        )

    return {
        "folders_scanned": len(candidates),
        "distinct_drives": len(groups),
        "duplicate_groups": len(dup_groups),
        "folders_recommended_to_keep": len(groups),
        "folders_recommended_to_move": sum(len(g["recommend_move"]) for g in groups),
        "folders_with_run_suffix": sum(1 for c in candidates if c["run_suffix"] is not None),
        "folders_without_manifest": sum(
            1 for c in candidates if not c["has_manifest"] or c["manifest_error"]
        ),
        "per_root": per_root,
    }


def render_markdown(report: dict[str, Any]) -> str:
    s = report["summary"]
    L: list[str] = []
    add = L.append

    add("# Duplicate drive folders on disk")
    add("")
    add(f"Generated {report['generated_utc']} (UTC). **Nothing was deleted, "
        "moved or renamed.** Everything below is a recommendation.")
    add("")
    add("The processing script used to add `_02` or `_03` to a drive's folder "
        "name whenever a folder of that name already existed, so the same drive "
        "can sit on disk two or three times over. Those copies are grouped back "
        "together here and compared on how complete each one is: whether it has "
        "a manifest (the index file without which the folder cannot be loaded "
        "at all), how many video clips it lists, how much GPS and accelerometer "
        "data survived, and how many preview images it has.")
    add("")

    add("## The numbers")
    add("")
    add(f"- Folders looked at: **{s['folders_scanned']}**")
    add(f"- Distinct drives they represent: **{s['distinct_drives']}**")
    add(f"- Drives stored more than once: **{s['duplicate_groups']}**")
    add(f"- Folders worth keeping (one per drive): **{s['folders_recommended_to_keep']}**")
    add(f"- Folders recommended for the trash folder: **{s['folders_recommended_to_move']}**")
    add(f"- Folders whose name carries an `_02` style suffix: **{s['folders_with_run_suffix']}**")
    add(f"- Folders with no usable manifest: **{s['folders_without_manifest']}**")
    add("")
    add("| Location | Folders | Distinct drives | Stored more than once | With `_NN` suffix | No manifest | To move |")
    add("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for root, d in sorted(s["per_root"].items()):
        add(f"| `{root}` | {d['folders']} | {d['groups']} | {d['duplicate_groups']} | "
            f"{d['with_suffix']} | {d['no_manifest']} | {d['recommend_move']} |")
    add("")

    add("## Drives stored more than once")
    add("")
    dup = [g for g in report["groups"] if g["is_duplicate_group"]]
    if not dup:
        add("None. Every drive appears exactly once.")
    else:
        add(f"{len(dup)} drive(s) have more than one folder. For each, keep the "
            "row marked **keep** and move the others to a trash folder so they "
            "can be checked before anything is thrown away.")
        for g in dup:
            add("")
            add(f"### `{g['base_tag']}` ({g['view']}) -- {g['candidate_count']} copies")
            add("")
            add("| | Folder | Manifest | Clips | GPS rows | Accel rows | Preview images | Size (MB) |")
            add("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |")
            for c in g["candidates"]:
                mark = "**keep**" if c["folder"] == g["keep"] else "move"
                manifest = "yes" if c["has_manifest"] and not c["manifest_error"] else "**missing**"
                add(f"| {mark} | `{c['folder']}` | {manifest} | "
                    f"{c['clip_count'] if c['clip_count'] is not None else '-'} | "
                    f"{c['gnss_rows'] if c['gnss_rows'] is not None else '-'} | "
                    f"{c['accel_rows'] if c['accel_rows'] is not None else '-'} | "
                    f"{c['thumb_count']} | {c['size_bytes'] / 1e6:.1f} |")
            add("")
            for mv in g["recommend_move"]:
                add(f"- Move `{mv['folder']}` -> `{mv['to']}` ({mv['why']})")
    add("")

    add("## Folders with no manifest")
    add("")
    no_man = [
        c for g in report["groups"] for c in g["candidates"]
        if not c["has_manifest"] or c["manifest_error"]
    ]
    if not no_man:
        add("None. Every folder has a manifest.")
    else:
        add("A folder without a manifest cannot be loaded into the database as "
            "it stands, because the manifest is what lists the clips and points "
            "at the telemetry files. These still hold their raw data, so they "
            "are not worthless -- they just need re-processing, or they are the "
            "discardable half of a duplicate pair.")
        add("")
        add("| Folder | Clips | GPS rows | Accel rows | Preview images | Also stored elsewhere |")
        add("| --- | ---: | ---: | ---: | ---: | --- |")
        for c in no_man:
            g = next(
                gg for gg in report["groups"]
                if any(x["folder"] == c["folder"] for x in gg["candidates"])
            )
            others = [x["folder"] for x in g["candidates"] if x["folder"] != c["folder"]]
            add(f"| `{c['folder']}` | - | "
                f"{c['gnss_rows'] if c['gnss_rows'] is not None else '-'} | "
                f"{c['accel_rows'] if c['accel_rows'] is not None else '-'} | "
                f"{c['thumb_count']} | "
                f"{', '.join('`' + o + '`' for o in others) if others else 'no, this is the only copy'} |")
    add("")

    if report["skipped"]:
        add("## Skipped")
        add("")
        for sk in report["skipped"]:
            add(f"- `{sk['path']}` -- {sk['reason']}")
        add("")

    add("## What happens next")
    add("")
    add("Nothing automatically. This tool only looks and reports. Acting on the "
        "recommendations means moving the listed folders into a trash folder "
        "alongside the originals, leaving one folder per drive, and checking "
        "the result before deleting anything permanently.")
    add("")
    return "\n".join(L)


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Report duplicate processed-drive folders. Never deletes anything.",
    )
    p.add_argument(
        "--root",
        action="append",
        required=True,
        dest="roots",
        help="A folder containing processed drive folders. Repeatable.",
    )
    p.add_argument(
        "--report-dir",
        required=True,
        help="Directory the report files are written to.",
    )
    p.add_argument(
        "--trash-name",
        default=DEFAULT_TRASH_DIRNAME,
        help=f"Name of the recommended trash folder (default: {DEFAULT_TRASH_DIRNAME}).",
    )
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    global DEFAULT_TRASH_DIRNAME
    DEFAULT_TRASH_DIRNAME = args.trash_name

    roots = [Path(r) for r in args.roots]
    candidates, skipped = scan_roots(roots)
    groups = group_candidates(candidates)

    report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "roots": [str(r) for r in roots],
        "trash_folder_name": DEFAULT_TRASH_DIRNAME,
        "summary": summarise(groups, candidates),
        "groups": groups,
        "skipped": skipped,
    }

    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = report_dir / f"dedupe_folders_{stamp}.json"
    md_path = report_dir / f"dedupe_folders_{stamp}.md"

    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")

    s = report["summary"]
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    print(
        f"folders={s['folders_scanned']} drives={s['distinct_drives']} "
        f"duplicate_groups={s['duplicate_groups']} "
        f"keep={s['folders_recommended_to_keep']} "
        f"move={s['folders_recommended_to_move']} "
        f"no_manifest={s['folders_without_manifest']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
