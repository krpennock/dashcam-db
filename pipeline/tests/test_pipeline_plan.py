"""Tests for the pipeline's drive planning and delivery packaging.

Standard library only:

    python -m unittest discover -s pipeline/tests -v

The planning tests matter more than they look: they decide whether a
re-processed drive updates the one already in the database or becomes a second
copy of it, which is the fault this whole project exists to fix.
"""
from __future__ import annotations

import json
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashcam_pipeline import delivery, plan  # noqa: E402
from dashcam_pipeline.ledger import Ledger  # noqa: E402


def pair(ts: str, kind: str = "N") -> list[str]:
    """The two clips a camera writes at one moment: front and rear."""
    return [f"{ts}_{kind}F", f"{ts}_{kind}R"]


class GroupingTests(unittest.TestCase):
    def test_clips_a_minute_apart_are_one_drive(self):
        stems = pair("20260806_094436") + pair("20260806_094537") + pair("20260806_094638")
        groups = plan.group_into_drives(stems, gap_seconds=120)
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]), 6)

    def test_a_long_gap_starts_a_new_drive(self):
        stems = pair("20260806_094436") + pair("20260806_120000")
        groups = plan.group_into_drives(stems, gap_seconds=120)
        self.assertEqual([len(g) for g in groups], [2, 2])

    def test_a_front_and_rear_pair_is_not_two_recordings(self):
        groups = plan.group_into_drives(pair("20260806_094436"), gap_seconds=120)
        self.assertEqual(len(groups), 1)

    def test_unparseable_names_are_ignored(self):
        groups = plan.group_into_drives(["not-a-clip", *pair("20260806_094436")], gap_seconds=120)
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]), 2)


class ClassifyTests(unittest.TestCase):
    def test_parking_clips_alone_are_not_a_drive(self):
        stems = pair("20260806_095427", "P") + pair("20260806_095527", "P") + pair("20260806_095627", "P")
        kind, counts, fronts = plan.classify(stems)
        self.assertEqual(kind, "parking_only")
        self.assertEqual(counts["P"], 6)
        self.assertEqual(fronts, 3)

    def test_too_few_clips_is_skipped_as_short(self):
        stems = pair("20260806_094436") + pair("20260806_094537")
        kind, _, fronts = plan.classify(stems, min_pairs=3)
        self.assertEqual(kind, "skipped_short")
        self.assertEqual(fronts, 2)

    def test_an_event_clip_counts_as_driving(self):
        stems = pair("20260806_094436", "E") + pair("20260806_094537", "P") + pair("20260806_094638", "P")
        kind, _, _ = plan.classify(stems, min_pairs=3)
        self.assertEqual(kind, "drive")


class MatchingTests(unittest.TestCase):
    """The five outcomes, against a ledger holding one known drive."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.led = Ledger(Path(self.tmp.name) / "state.sqlite")
        self.addCleanup(self.led.close)

        self.known = pair("20260806_094436") + pair("20260806_094537") + pair("20260806_094638")
        self.led.record_drive("camry", "20260806_094436_camry", status="ingested")
        self.led.set_drive_clips("camry", "20260806_094436_camry", self.known)

    def test_clips_we_have_never_seen_are_a_new_drive(self):
        match, tag, _ = plan.match_known(self.led, "camry", pair("20260901_100000"))
        self.assertEqual(match, plan.NEW)
        self.assertIsNone(tag)

    def test_exactly_the_same_clips_are_unchanged(self):
        match, tag, _ = plan.match_known(self.led, "camry", self.known)
        self.assertEqual(match, plan.UNCHANGED)
        self.assertEqual(tag, "20260806_094436_camry")

    def test_extra_clips_extend_the_drive_we_already_have(self):
        match, tag, why = plan.match_known(self.led, "camry", self.known + pair("20260806_094739"))
        self.assertEqual(match, plan.EXTENDED)
        self.assertEqual(tag, "20260806_094436_camry")
        self.assertIn("arrived", why)

    def test_a_partly_overwritten_drive_is_a_remnant(self):
        match, tag, why = plan.match_known(self.led, "camry", self.known[2:])
        self.assertEqual(match, plan.REMNANT)
        self.assertEqual(tag, "20260806_094436_camry")
        self.assertIn("no longer on the card", why)

    def test_clips_spanning_two_known_drives_need_a_look(self):
        other = pair("20260806_120000")
        self.led.record_drive("camry", "20260806_120000_camry", status="ingested")
        self.led.set_drive_clips("camry", "20260806_120000_camry", other)

        match, _, why = plan.match_known(self.led, "camry", self.known + other)
        self.assertEqual(match, plan.NEEDS_REVIEW)
        self.assertIn("span", why)


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.led = Ledger(Path(self.tmp.name) / "state.sqlite")
        self.addCleanup(self.led.close)

    def test_an_extended_drive_keeps_its_original_tag(self):
        """The database id comes from the tag, so the tag must not move."""
        known = pair("20260806_094436") + pair("20260806_094537") + pair("20260806_094638")
        self.led.record_drive("camry", "20260806_094436_camry", status="ingested")
        self.led.set_drive_clips("camry", "20260806_094436_camry", known)

        # An earlier clip turns up that we did not have last time.
        stems = pair("20260806_094335") + known
        plans = plan.plan_drives(self.led, "camry", stems)

        self.assertEqual(len(plans), 1)
        got = plans[0]
        self.assertEqual(got.match, plan.EXTENDED)
        self.assertEqual(got.drive_tag, "20260806_094436_camry")
        self.assertTrue(got.should_build)

    def test_a_brand_new_drive_is_tagged_from_its_first_clip(self):
        stems = pair("20260901_081500") + pair("20260901_081601") + pair("20260901_081702")
        plans = plan.plan_drives(self.led, "civic", stems)
        self.assertEqual(plans[0].drive_tag, "20260901_081500_civic")
        self.assertEqual(plans[0].match, plan.NEW)
        self.assertTrue(plans[0].should_build)

    def test_parked_and_short_groups_are_not_built(self):
        stems = (
            pair("20260901_081500", "P") + pair("20260901_081601", "P") + pair("20260901_081702", "P")
            + pair("20260901_120000") + pair("20260901_120101")
        )
        plans = plan.plan_drives(self.led, "civic", stems)
        kinds = sorted(p.classification for p in plans)
        self.assertEqual(kinds, ["parking_only", "skipped_short"])
        self.assertFalse(any(p.should_build for p in plans))

    def test_clip_set_hash_ignores_order(self):
        a = plan.clip_set_sha1(["b", "a"])
        b = plan.clip_set_sha1(["a", "b"])
        self.assertEqual(a, b)
        self.assertNotEqual(a, plan.clip_set_sha1(["a"]))


class DeliveryTests(unittest.TestCase):
    def _make_drive(self, root: Path) -> Path:
        drive = root / "camry" / "20260806_094436_camry_front"
        (drive / "artifacts" / "thumbs").mkdir(parents=True)
        (drive / "artifacts" / "clips").mkdir(parents=True)

        (drive / "manifest.json").write_text(json.dumps({
            "drive_tag": "20260806_094436_camry",
            "vehicle_tag": "camry",
            "source": {"serial": "ELT9K1OBE00076", "model": "ELITE9-2CH"},
            "session_time": {"time_confidence": "gnss_median"},
            "telemetry": {"gnss": {"rows": 577}, "accel": {"rows": 7484}},
            "clips": [
                {"channel": "front", "clip_name": "20260806_094436_NF"},
                {"channel": "rear", "clip_name": "20260806_094436_NR"},
            ],
        }), encoding="utf-8")
        (drive / "artifacts" / "thumbs" / "one.jpg").write_bytes(b"thumbnail")
        # The raw clips are the bulk of a folder and must not be sent.
        (drive / "artifacts" / "clips" / "20260806_094436_NF.nmea").write_bytes(b"x" * 4096)
        return drive

    def test_a_delivery_carries_the_drive_but_not_its_raw_clips(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "src"
            drive = self._make_drive(root)
            out = Path(tmp) / "out"
            out.mkdir()

            d = delivery.build([drive], vehicle_tag="camry", source_kind="sdcard",
                               source_key="E:", workdir=out)

            with tarfile.open(d.tar_path) as tar:
                names = sorted(tar.getnames())
            self.assertIn("batch.json", names)
            self.assertIn("SHA256SUMS", names)
            self.assertIn("camry/20260806_094436_camry_front/manifest.json", names)
            self.assertIn("camry/20260806_094436_camry_front/artifacts/thumbs/one.jpg", names)
            self.assertFalse([n for n in names if "artifacts/clips" in n],
                             "the raw clips should never be sent")

    def test_the_batch_describes_the_drive_and_the_camera(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "src"
            drive = self._make_drive(root)
            out = Path(tmp) / "out"
            out.mkdir()

            d = delivery.build([drive], vehicle_tag="camry", workdir=out)
            item = d.batch["items"][0]

            self.assertEqual(d.batch["camera"]["serial"], "ELT9K1OBE00076")
            self.assertEqual(item["drive_tag"], "20260806_094436_camry")
            self.assertEqual(item["view"], "front")
            self.assertEqual(item["status"], "processed")
            self.assertEqual(item["clip_pairs"], 1)
            self.assertEqual(item["time_confidence"], "gnss_median")
            self.assertEqual(item["folder"], "camry/20260806_094436_camry_front")

    def test_drives_the_pc_chose_not_to_send_still_travel(self):
        """The Imports page should show everything that was on the card."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "src"
            drive = self._make_drive(root)
            out = Path(tmp) / "out"
            out.mkdir()

            d = delivery.build(
                [drive], vehicle_tag="camry", workdir=out,
                recorded_items=[{
                    "vehicle_tag": "camry", "drive_tag": "20260806_095427_camry",
                    "view": "front", "status": "parking_only",
                    "reason": "no driving clips in this group",
                }],
            )
            statuses = sorted(i["status"] for i in d.batch["items"])
            self.assertEqual(statuses, ["parking_only", "processed"])

    def test_every_file_is_checksummed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "src"
            drive = self._make_drive(root)
            out = Path(tmp) / "out"
            out.mkdir()

            d = delivery.build([drive], vehicle_tag="camry", workdir=out)
            with tarfile.open(d.tar_path) as tar:
                sums = tar.extractfile("SHA256SUMS").read().decode("utf-8")
                names = {n for n in tar.getnames() if n != "SHA256SUMS"}

            listed = {line.split("  ", 1)[1] for line in sums.strip().splitlines()}
            self.assertEqual(listed, names)


if __name__ == "__main__":
    unittest.main()
