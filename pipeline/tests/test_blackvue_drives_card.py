"""Tests for the parts of blackvue_drives.py that let a clip be read in place:
the embedded telemetry reader and the seek-based still schedule.

Standard library only:

    python -m unittest discover -s pipeline/tests -v
"""
from __future__ import annotations

import importlib.util
import struct
import sys
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "blackvue_drives.py"
if "blackvue_drives" in sys.modules:
    bd = sys.modules["blackvue_drives"]
else:
    _spec = importlib.util.spec_from_file_location("blackvue_drives", MODULE_PATH)
    bd = importlib.util.module_from_spec(_spec)
    sys.modules["blackvue_drives"] = bd
    assert _spec.loader is not None
    _spec.loader.exec_module(bd)


def atom(kind: bytes, payload: bytes) -> bytes:
    return struct.pack(">I4s", 8 + len(payload), kind) + payload


def block(tag: bytes, payload: bytes) -> bytes:
    """A telemetry block: length counting itself, four-byte tag, payload."""
    return struct.pack(">I", 8 + len(payload)) + tag + payload


#: As the camera writes it: a bare timestamp marker line, then the sentences.
NMEA = (b"[1754491476000]\r\n"
        b"[1754491476000]$GPRMC,142436.00,A,3010.1234,N,09742.5678,W,10.5,180.0,060826,,,A*5C\r\n"
        b"[1754491477000]$GPGGA,142437.00,3010.1234,N,09742.5678,W,1,09,0.9,150.0,M,-22.0,M,,*47\r\n")

#: Three accelerometer samples, then the all-ones end mark the camera writes.
ACCEL = (struct.pack(">IHHH", 1043, 0x007E, 0xFFFC, 0x0013)
         + struct.pack(">IHHH", 1243, 0x0080, 0xFFFE, 0x0010)
         + struct.pack(">IHHH", 1443, 0x007F, 0x0001, 0x0012)
         + b"\xff" * 10)


def fake_clip(path: Path, *, with_padding_first: bool = False, telemetry: bool = True) -> Path:
    """A file laid out like a BlackVue clip, with a huge video atom we must never read."""
    parts = [atom(b"ftyp", b"mp42\x00\x00\x00\x00mp42isom")]
    if with_padding_first:
        parts.append(atom(b"free", b"\x00" * 64))
    if telemetry:
        data = (block(b"sttm", b"\x00" * 8)
                + block(b"gps ", NMEA + b"\x00" + b"\x00" * 16)
                + block(b"3gf ", ACCEL + b"\x00" * 6)
                + block(b"thum", struct.pack(">I", 4) + b"\xff\xd8\xff\xd9"))
        parts.append(atom(b"free", data))
    parts.append(atom(b"moov", b"\x00" * 16))
    # The video: a header claiming two gigabytes, with nothing behind it. The
    # reader has to stop at the header or it would fail here.
    parts.append(struct.pack(">I4s", 0x7FFFFFF0, b"mdat"))
    path.write_bytes(b"".join(parts))
    return path


class EmbeddedTelemetryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.out = self.root / "clips"

    def test_gps_and_accelerometer_come_out_as_blackclue_would_write_them(self):
        clip = fake_clip(self.root / "20260806_094436_NF.mp4")

        written = bd.extract_embedded_telemetry(clip, self.out)

        self.assertEqual(set(written), {".nmea", ".3gf", ".3gf.txt"})
        self.assertEqual((self.out / "20260806_094436_NF.nmea").read_bytes(), NMEA,
                         "the GPS text up to its terminating zero, nothing more")
        self.assertEqual((self.out / "20260806_094436_NF.3gf").read_bytes(), ACCEL[:-10],
                         "the raw samples up to the end mark")
        rows = bd.parse_3gf_txt(self.out / "20260806_094436_NF.3gf.txt")
        self.assertEqual(
            [(r["t_ms"], r["ax"], r["ay"], r["az"]) for r in rows],
            [(1043, 126, -4, 19), (1243, 128, -2, 16), (1443, 127, 1, 18)],
        )

    def test_the_text_layout_matches_blackclue_line_for_line(self):
        # The example line in parse_3gf_txt's docstring, produced from its bytes.
        line = bd.format_accel_samples(struct.pack(">IHHH", 1043, 0x007E, 0xFFFC, 0x0013)).rstrip("\n")
        self.assertEqual(line, "       0 00000413 007e fffc 0013   1043    126     -4     19")

    def test_nothing_is_written_beside_the_clip(self):
        clip = fake_clip(self.root / "20260806_094436_NF.mp4")

        bd.extract_embedded_telemetry(clip, self.out)

        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ["20260806_094436_NF.mp4", "clips"])

    def test_a_padding_atom_before_the_telemetry_is_skipped(self):
        clip = fake_clip(self.root / "20260806_094436_NF.mp4", with_padding_first=True)

        written = bd.extract_embedded_telemetry(clip, self.out)

        self.assertIn(".nmea", written)

    def test_a_clip_without_telemetry_yields_nothing_rather_than_an_error(self):
        clip = fake_clip(self.root / "20260806_094436_NF.mp4", telemetry=False)

        written = bd.extract_embedded_telemetry(clip, self.out)

        self.assertEqual(written, {})
        self.assertFalse(self.out.exists(), "no folder is made for nothing")

    def test_the_video_itself_is_never_read(self):
        clip = fake_clip(self.root / "20260806_094436_NF.mp4")
        # The file is a few hundred bytes but claims a two-gigabyte video atom.
        # Reading it would raise or hang; the reader must stop at the header.
        self.assertIsNotNone(bd.read_embedded_data(clip))

    def test_the_nmea_feeds_the_existing_stitching_and_parsing(self):
        clip = fake_clip(self.root / "20260806_094436_NF.mp4")
        written = bd.extract_embedded_telemetry(clip, self.out)

        records = bd.nmea_to_records(written[".nmea"].read_text(encoding="utf-8"))

        self.assertEqual(len(records), 2)
        self.assertEqual(bd.nmea_first_epoch_ms(written[".nmea"]), 1754491476000)


class StillScheduleTests(unittest.TestCase):
    def _segments(self, durations):
        # Consecutive clips a minute apart, as the camera names them.
        paths = [Path(f"20260806_09{44 + i:02d}36_NF.mp4") for i in range(len(durations))]
        return bd.build_clip_segments(
            paths, camera_tz=bd.parse_utc_offset("-06:00"), clock_offset_s=0.0,
            clip_seconds=60.0, durations=dict(zip(paths, durations)),
        )

    def test_each_still_is_taken_from_its_own_clip_at_the_right_moment(self):
        segs = self._segments([61.0, 43.0, 61.0])   # a full clip, one cut short, a full clip

        plan = bd.still_schedule(segs, 30.0)

        self.assertEqual(
            [(n, seg.name, within) for n, seg, within in plan],
            [
                (1, segs[0].path.name, 0.0),
                (2, segs[0].path.name, 30.0),
                (3, segs[0].path.name, 60.0),
                (4, segs[1].path.name, 29.0),     # 90 s in: 29 s into the short clip
                (5, segs[2].path.name, 16.0),     # 120 s in: the short clip ended at 104 s
                (6, segs[2].path.name, 46.0),
            ],
        )

    def test_a_tick_in_a_clips_final_half_second_is_pulled_back(self):
        segs = self._segments([60.2])

        plan = bd.still_schedule(segs, 60.0)

        self.assertEqual(len(plan), 2)
        self.assertAlmostEqual(plan[1][2], 59.7, places=3)

    def test_the_still_numbers_match_what_the_renamer_assumes(self):
        # rename_stills_with_timestamps reads still n as (n-1) * every_seconds on
        # the stitched timeline; the schedule must agree or the index lies.
        segs = self._segments([61.0, 61.0])
        plan = bd.still_schedule(segs, 30.0)
        for n, _, _ in plan:
            seg, within = bd.segment_for_concat_offset(segs, (n - 1) * 30.0)
            planned_seg, planned_within = plan[n - 1][1], plan[n - 1][2]
            self.assertEqual(planned_seg, seg.path)
            self.assertAlmostEqual(planned_within, min(within, seg.duration_s - 0.5), places=6)

    def test_a_cap_on_frames_is_respected(self):
        segs = self._segments([61.0, 61.0, 61.0])
        self.assertEqual(len(bd.still_schedule(segs, 30.0, max_frames=2)), 2)

    def test_no_clips_means_no_stills(self):
        self.assertEqual(bd.still_schedule([], 30.0), [])

    def test_a_dry_run_counts_without_ffmpeg(self):
        segs = self._segments([61.0, 61.0])
        n = bd.run_ffmpeg_stills(
            ffmpeg_exe="definitely-not-ffmpeg", segments=segs, out_dir=Path("nowhere"),
            every_seconds=30.0, width=1920, fmt="jpg", jpg_quality=3, max_frames=0,
            jobs=1, dry_run=True, verbose=False,
        )
        self.assertEqual(n, 5)


if __name__ == "__main__":
    unittest.main()
