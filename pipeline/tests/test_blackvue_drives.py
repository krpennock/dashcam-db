"""Unit tests for pipeline/blackvue_drives.py.

Standard library only (unittest), so they run anywhere the pipeline runs:

    python -m unittest discover -s pipeline/tests -v
"""
from __future__ import annotations

import datetime as _dt
import importlib.util
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "blackvue_drives.py"
_spec = importlib.util.spec_from_file_location("blackvue_drives", MODULE_PATH)
bd = importlib.util.module_from_spec(_spec)
sys.modules["blackvue_drives"] = bd
assert _spec.loader is not None
_spec.loader.exec_module(bd)

GNSS_HEADER = (
    "ms,t_rel_s,utc_time,lat,lon,speed_knots,speed_mps,speed_mph,course_deg,"
    "rmc_status,fix_quality,satellites,hdop,alt_m,geoid_sep_m,date_ddmmyy,time_hhmmss\n"
)


def write_gnss_csv(path: Path, rows) -> Path:
    """rows: iterable of (t_rel_s, device_utc datetime, rmc_status)."""
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write(GNSS_HEADER)
        for t_rel, dev, status in rows:
            f.write(
                f"{int(1000 * t_rel)},{t_rel},{dev.isoformat().replace('+00:00', 'Z')},"
                f"30.1,-97.7,10,5.1,11.5,180,{status},1,9,0.9,150,-22,"
                f"{dev.strftime('%d%m%y')},{dev.strftime('%H%M%S')}\n"
            )
    return path


class DriveIdentityTests(unittest.TestCase):
    def test_drive_session_id_is_stable_and_uuid5(self):
        sid = bd._stable_drive_session_id(vehicle_tag="camry", drive_tag="20260806_094436_camry")
        self.assertEqual(sid, bd._stable_drive_session_id(vehicle_tag="camry", drive_tag="20260806_094436_camry"))
        self.assertEqual(
            sid,
            str(uuid.uuid5(uuid.NAMESPACE_URL, "blackvue-drive:camry:20260806_094436_camry")),
        )

    def test_suffixed_tag_produces_a_different_id(self):
        # The _02 suffix bug is what put duplicate copies of one drive in the DB.
        clean = bd._stable_drive_session_id(vehicle_tag="camry", drive_tag="20260806_094436_camry")
        suffixed = bd._stable_drive_session_id(vehicle_tag="camry", drive_tag="20260806_094436_camry_02")
        self.assertNotEqual(clean, suffixed)

    def test_clip_set_hash_ignores_order_but_not_membership(self):
        a = bd.clip_set_sha1(["20260806_094436_IF", "20260806_094536_NF"])
        b = bd.clip_set_sha1(["20260806_094536_NF", "20260806_094436_IF"])
        c = bd.clip_set_sha1(["20260806_094436_IF"])
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)

    def test_clip_type_from_name(self):
        self.assertEqual(bd.clip_type_from_name("20260806_094436_NF.mp4"), "N")
        self.assertEqual(bd.clip_type_from_name("20260806_094436_PR.mp4"), "P")
        self.assertEqual(bd.clip_type_from_name("20260806_094436_IF"), "I")
        self.assertIsNone(bd.clip_type_from_name("not-a-clip.mp4"))


class UtcOffsetTests(unittest.TestCase):
    def test_round_trip(self):
        for text in ("-06:00", "+05:30", "Z"):
            tz = bd.parse_utc_offset(text)
            expect = "+00:00" if text == "Z" else text
            self.assertEqual(bd.format_utc_offset(tz), expect)

    def test_compact_form(self):
        self.assertEqual(bd.format_utc_offset(bd.parse_utc_offset("-0600")), "-06:00")

    def test_rejects_garbage(self):
        with self.assertRaises(ValueError):
            bd.parse_utc_offset("America/Chicago")


class SessionTimeTests(unittest.TestCase):
    """The camera clock is UTC-6 with no daylight saving."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        # A drive whose first clip is named 09:44:36 camera time = 15:44:36 UTC.
        self.clips = [Path(f"20260806_09{m:02d}{s:02d}_NF.mp4") for m, s in ((44, 36), (45, 37), (46, 38))]
        self.file_start = _dt.datetime(2026, 8, 6, 15, 44, 36, tzinfo=_dt.timezone.utc)

    def test_gnss_median_ignores_stale_rows(self):
        """Stale pre-lock rows report the last fix's time but still say "A"."""
        true_start = self.file_start + _dt.timedelta(seconds=6)
        rows = []
        for i in range(5):  # stale: an hour old, as after waking from parking
            rows.append((float(i), true_start - _dt.timedelta(hours=1) + _dt.timedelta(seconds=i), "A"))
        for i in range(5, 200):
            rows.append((float(i), true_start + _dt.timedelta(seconds=i), "A"))
        csv_path = write_gnss_csv(self.dir / "drive.csv", rows)

        st = bd.compute_session_time(
            gnss_csv=csv_path, accel_csv=None, clip_paths=self.clips,
            camera_tz=bd.parse_utc_offset("-06:00"), clip_seconds=60.0,
        )
        self.assertEqual(st["time_confidence"], "gnss_median")
        self.assertEqual(st["start_ts_utc"], "2026-08-06T15:44:42Z")
        self.assertEqual(st["gnss_valid_rows"], 200)
        self.assertEqual(st["clock_offset_s"], 6.0)

    def test_too_few_fixes_falls_back_to_filenames(self):
        rows = [(float(i), self.file_start + _dt.timedelta(seconds=i), "A") for i in range(5)]
        rows += [(float(i), self.file_start + _dt.timedelta(seconds=i), "V") for i in range(5, 100)]
        csv_path = write_gnss_csv(self.dir / "drive.csv", rows)

        st = bd.compute_session_time(
            gnss_csv=csv_path, accel_csv=None, clip_paths=self.clips,
            camera_tz=bd.parse_utc_offset("-06:00"), clip_seconds=60.0,
        )
        self.assertEqual(st["time_confidence"], "filename_low")
        self.assertEqual(st["start_ts_utc"], "2026-08-06T15:44:36Z")

    def test_no_gnss_at_all_falls_back_to_filenames(self):
        st = bd.compute_session_time(
            gnss_csv=None, accel_csv=None, clip_paths=self.clips,
            camera_tz=bd.parse_utc_offset("-06:00"), clip_seconds=60.0,
        )
        self.assertEqual(st["time_confidence"], "filename_low")
        self.assertEqual(st["start_ts_utc"], "2026-08-06T15:44:36Z")
        self.assertEqual(st["camera_utc_offset"], "-06:00")

    def test_badly_set_camera_clock_is_flagged_but_gnss_still_wins(self):
        """January 2026: one camera was set five hours off; GPS was right."""
        true_start = self.file_start + _dt.timedelta(hours=5)
        rows = [(float(i), true_start + _dt.timedelta(seconds=i), "A") for i in range(120)]
        csv_path = write_gnss_csv(self.dir / "drive.csv", rows)

        st = bd.compute_session_time(
            gnss_csv=csv_path, accel_csv=None, clip_paths=self.clips,
            camera_tz=bd.parse_utc_offset("-06:00"), clip_seconds=60.0,
        )
        self.assertEqual(st["time_confidence"], "gnss_median_clock_mismatch")
        self.assertEqual(st["start_ts_utc"], "2026-08-06T20:44:36Z")
        self.assertEqual(st["filename_delta_s"], 5 * 3600.0)


class ClipTimelineTests(unittest.TestCase):
    def test_short_clip_and_parking_gap_keep_stills_on_real_time(self):
        camera_tz = bd.parse_utc_offset("-06:00")
        # 09:44:36 runs a full 61 s; 09:45:37 is cut short at 43 s by an event;
        # then nothing until a parking clip 100 s later.
        paths = [
            Path("20260806_094436_NF.mp4"),
            Path("20260806_094537_NF.mp4"),
            Path("20260806_094720_PF.mp4"),
        ]
        durations = {paths[0]: 61.0, paths[1]: 43.0, paths[2]: 61.0}
        segs = bd.build_clip_segments(
            paths, camera_tz=camera_tz, clock_offset_s=0.0, clip_seconds=60.0, durations=durations
        )
        self.assertEqual([s.concat_start_s for s in segs], [0.0, 61.0, 104.0])

        # 30 s into the stitched video is still the first clip.
        seg, within = bd.segment_for_concat_offset(segs, 30.0)
        self.assertEqual(seg.path, paths[0])
        self.assertEqual(within, 30.0)

        # 120 s in lands in the parking clip, which really started 163 s after the
        # drive began: assuming 60 s clips would have put it 43 s earlier.
        seg, within = bd.segment_for_concat_offset(segs, 120.0)
        self.assertEqual(seg.path, paths[2])
        self.assertEqual(within, 16.0)
        self.assertEqual(
            (seg.start_utc + _dt.timedelta(seconds=within)).isoformat(),
            "2026-08-06T15:47:36+00:00",
        )

    def test_clock_offset_shifts_every_clip(self):
        segs = bd.build_clip_segments(
            [Path("20260806_094436_NF.mp4")],
            camera_tz=bd.parse_utc_offset("-06:00"), clock_offset_s=6.0,
            clip_seconds=60.0, durations=None,
        )
        self.assertEqual(segs[0].start_utc.isoformat(), "2026-08-06T15:44:42+00:00")


class RebuildDecisionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.final = self.dir / "20260806_094436_camry_front"
        self.stems = ["20260806_094436_NF", "20260806_094436_NR"]

    def test_missing_folder_builds(self):
        action, _ = bd.drive_build_decision(
            self.final, manifest_name="manifest.json", clip_stems=self.stems, signature="sig"
        )
        self.assertEqual(action, "build")

    def test_marker_match_skips(self):
        self.final.mkdir()
        bd.write_complete_marker(self.final, {"clip_set_sha1": bd.clip_set_sha1(self.stems), "args_signature": "sig"})
        action, _ = bd.drive_build_decision(
            self.final, manifest_name="manifest.json", clip_stems=self.stems, signature="sig"
        )
        self.assertEqual(action, "skip")

    def test_new_clip_or_changed_options_rebuild(self):
        self.final.mkdir()
        bd.write_complete_marker(self.final, {"clip_set_sha1": bd.clip_set_sha1(self.stems), "args_signature": "sig"})
        action, _ = bd.drive_build_decision(
            self.final, manifest_name="manifest.json",
            clip_stems=self.stems + ["20260806_094537_NF"], signature="sig",
        )
        self.assertEqual(action, "build")
        action, _ = bd.drive_build_decision(
            self.final, manifest_name="manifest.json", clip_stems=self.stems, signature="other",
        )
        self.assertEqual(action, "build")

    def test_folder_from_before_the_marker_is_adopted(self):
        self.final.mkdir()
        (self.final / "manifest.json").write_text(
            '{"clips": [{"clip_name": "20260806_094436_NF"}, {"clip_name": "20260806_094436_NR"}]}',
            encoding="utf-8",
        )
        action, _ = bd.drive_build_decision(
            self.final, manifest_name="manifest.json", clip_stems=self.stems, signature="sig"
        )
        self.assertEqual(action, "adopt")


class AtomicSwapTests(unittest.TestCase):
    def test_swap_replaces_previous_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            work = root / "_work"
            work.mkdir()
            final = root / "drive_front"
            final.mkdir()
            (final / "manifest.json").write_text("old", encoding="utf-8")
            build = work / "drive_front.build"
            build.mkdir()
            (build / "manifest.json").write_text("new", encoding="utf-8")

            bd.swap_build_into_place(build, final, retire_root=work)

            self.assertEqual((final / "manifest.json").read_text(encoding="utf-8"), "new")
            self.assertFalse(build.exists())
            self.assertEqual(list(work.glob("*.old-*")), [])


class ClipListTests(unittest.TestCase):
    def test_pairs_from_clip_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            names = ["20260806_094436_NF.mp4", "20260806_094436_NR.mp4", "20260806_094537_NF.mp4"]
            for n in names:
                (root / n).write_bytes(b"")
            listing = root / "clips.txt"
            listing.write_text(
                "# a comment\n\n" + "\n".join(str(root / n) for n in names) + "\n", encoding="utf-8"
            )

            pairs = bd.pairs_from_clip_list(listing)
            self.assertEqual([p.ts_key for p in pairs], ["20260806_094436", "20260806_094537"])
            self.assertIsNotNone(pairs[0].rear)
            self.assertIsNone(pairs[1].rear)

    def test_byte_order_mark_and_quotes_are_tolerated(self):
        """PowerShell's -Encoding UTF8 writes a BOM, and paths are often quoted."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            clip = root / "20260806_094436_NF.mp4"
            clip.write_bytes(b"")
            listing = root / "clips.txt"
            listing.write_text('﻿"' + str(clip) + '"\n', encoding="utf-8")

            pairs = bd.pairs_from_clip_list(listing)
            self.assertEqual(len(pairs), 1)
            self.assertEqual(pairs[0].front, clip)

    def test_missing_clip_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            listing = Path(tmp) / "clips.txt"
            listing.write_text(str(Path(tmp) / "nope_NF.mp4") + "\n", encoding="utf-8")
            with self.assertRaises(SystemExit):
                bd.pairs_from_clip_list(listing)


if __name__ == "__main__":
    unittest.main()
