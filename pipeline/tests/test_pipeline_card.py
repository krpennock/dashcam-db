"""Tests for reading a card in place: nothing copied, nothing written to it.

Standard library only:

    python -m unittest discover -s pipeline/tests -v
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashcam_pipeline import acquire, notify  # noqa: E402
from dashcam_pipeline.config import Vehicle  # noqa: E402
from dashcam_pipeline.ledger import Ledger  # noqa: E402
from dashcam_pipeline.sources import FolderSource  # noqa: E402


def fake_card(root: Path, stems) -> Path:
    """A folder that looks like a card: tiny files with real clip names."""
    folder = root / "BlackVue" / "Record"
    folder.mkdir(parents=True, exist_ok=True)
    for stem in stems:
        (folder / f"{stem}.mp4").write_bytes(b"x" * (len(stem) * 10))
    return folder


def listing(folder: Path) -> set[str]:
    return {p.name for p in folder.rglob("*")}


class RegisterTests(unittest.TestCase):
    """Noting what is on a card, without copying it."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.led = Ledger(root / "state.sqlite")
        self.addCleanup(self.led.close)
        self.vehicle = Vehicle(serial="ELT9K1OBE00076", tag="camry", output_root=root / "out")
        self.stems = ["20260806_094436_NF", "20260806_094436_NR", "20260806_094537_NF", "20260806_094537_NR"]
        self.folder = fake_card(root / "card", self.stems)
        self.source = FolderSource(self.folder, settle_seconds=0)

    def test_new_clips_are_recorded_as_seen_and_nothing_is_copied(self):
        before = listing(Path(self.tmp.name))

        result = acquire.register_new_clips(self.led, self.source, self.vehicle, log=lambda m: None)

        self.assertEqual(sorted(result.new_stems), sorted(self.stems))
        self.assertEqual(result.clips_on_source, 4)
        for stem in self.stems:
            row = self.led.get_clip("camry", stem)
            self.assertEqual(row["state"], "seen")
            self.assertEqual(row["size_bytes"], len(stem) * 10)
            self.assertEqual(row["source_kind"], "folder")
            self.assertIsNone(row["staged_path"])
            self.assertIsNone(row["held_path"])
        self.assertEqual(listing(Path(self.tmp.name)) - before, set(),
                         "nothing may be written anywhere by merely looking at a card")

    def test_a_second_look_finds_nothing_new(self):
        acquire.register_new_clips(self.led, self.source, self.vehicle, log=lambda m: None)

        again = acquire.register_new_clips(self.led, self.source, self.vehicle, log=lambda m: None)

        self.assertEqual(again.new_stems, [])
        self.assertEqual(again.clips_on_source, 4, "the clips are still on the card, just not new")

    def test_a_clip_processed_before_the_pipeline_existed_is_not_new(self):
        self.led.record_archived_clips("camry", ["20260806_094436_NF"])

        result = acquire.register_new_clips(self.led, self.source, self.vehicle, log=lambda m: None)

        self.assertNotIn("20260806_094436_NF", result.new_stems)
        self.assertEqual(len(result.new_stems), 3)

    def test_the_lookup_of_where_clips_are_points_at_the_card(self):
        result = acquire.register_new_clips(self.led, self.source, self.vehicle, log=lambda m: None)

        where = result.on_source
        self.assertEqual(where["20260806_094436_NF"], self.folder / "20260806_094436_NF.mp4")


class ClipPathTests(unittest.TestCase):
    """Where a clip can be read from: the card first, an old copy second."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.led = Ledger(self.root / "state.sqlite")
        self.addCleanup(self.led.close)

    def _held(self, stem: str) -> Path:
        path = self.root / "holding" / f"{stem}.mp4"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"held")
        self.led.record_clip(vehicle_tag="camry", stem=stem, ts_key=stem[:15], channel="front",
                             clip_type="N", size_bytes=4, state="held", held_path=str(path))
        return path

    def test_the_card_wins_when_the_clip_is_on_it(self):
        held = self._held("20260806_094436_NF")
        card_copy = self.root / "card" / "20260806_094436_NF.mp4"
        card_copy.parent.mkdir(parents=True)
        card_copy.write_bytes(b"card")

        paths = acquire.clip_paths(self.led, "camry", ["20260806_094436_NF"],
                                   {"20260806_094436_NF": card_copy})

        self.assertEqual(paths["20260806_094436_NF"], card_copy)
        self.assertNotEqual(paths["20260806_094436_NF"], held)

    def test_an_old_copy_is_used_when_the_card_no_longer_has_it(self):
        held = self._held("20260806_094436_NF")

        paths = acquire.clip_paths(self.led, "camry", ["20260806_094436_NF"], {})

        self.assertEqual(paths["20260806_094436_NF"], held)

    def test_a_clip_in_neither_place_is_left_out(self):
        self.led.record_clip(vehicle_tag="camry", stem="20260806_094436_NF", ts_key="20260806_094436",
                             channel="front", clip_type="N", size_bytes=1, state="seen")

        paths = acquire.clip_paths(self.led, "camry", ["20260806_094436_NF", "20260806_094537_NF"], {})

        self.assertEqual(paths, {}, "a seen clip has no copy; an unknown clip has nothing at all")


class EraseSafetyTests(unittest.TestCase):
    """When a card can go: nothing on it may still depend on it."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.led = Ledger(Path(self.tmp.name) / "state.sqlite")
        self.addCleanup(self.led.close)

    def _seen(self, stem: str) -> None:
        self.led.record_clip(vehicle_tag="camry", stem=stem, ts_key=stem[:15], channel="front",
                             clip_type="N", size_bytes=1, state="seen")

    def _drive(self, tag: str, status: str, stems) -> None:
        self.led.record_drive("camry", tag, status=status)
        self.led.set_drive_clips("camry", tag, stems)

    def test_a_clip_of_a_loaded_drive_no_longer_needs_the_card(self):
        self._seen("20260806_094436_NF")
        self._drive("20260806_094436_camry", "ingested", ["20260806_094436_NF"])

        self.assertEqual(self.led.unsettled_stems("camry", ["20260806_094436_NF"]), [])

    def test_parked_and_short_recordings_count_as_dealt_with(self):
        self._seen("20260806_095427_PF")
        self._seen("20260810_210322_NF")
        self._drive("20260806_095427_camry", "parking_only", ["20260806_095427_PF"])
        self._drive("20260810_210322_camry", "skipped_short", ["20260810_210322_NF"])

        self.assertEqual(self.led.unsettled_stems("camry", ["20260806_095427_PF", "20260810_210322_NF"]), [])

    def test_a_clip_whose_drive_is_still_in_flight_keeps_the_card(self):
        for stem, status in (("20260806_094436_NF", "planned"), ("20260806_104436_NF", "processed"),
                             ("20260806_114436_NF", "shipped"), ("20260806_124436_NF", "failed"),
                             ("20260806_134436_NF", "needs_review")):
            self._seen(stem)
            self._drive(f"{stem[:15]}_camry", status, [stem])

        waiting = self.led.unsettled_stems("camry", [
            "20260806_094436_NF", "20260806_104436_NF", "20260806_114436_NF",
            "20260806_124436_NF", "20260806_134436_NF",
        ])

        self.assertEqual(len(waiting), 5, "none of these has reached the database")

    def test_a_clip_no_drive_has_claimed_keeps_the_card(self):
        self._seen("20260806_094436_NF")

        self.assertEqual(self.led.unsettled_stems("camry", ["20260806_094436_NF"]), ["20260806_094436_NF"])

    def test_a_clip_we_never_saw_keeps_the_card(self):
        self.assertEqual(self.led.unsettled_stems("camry", ["20260806_094436_NF"]), ["20260806_094436_NF"])

    def test_video_copied_in_the_old_flow_is_safe_whatever_its_drive_says(self):
        self.led.record_clip(vehicle_tag="camry", stem="20260806_094436_NF", ts_key="20260806_094436",
                             channel="front", clip_type="N", size_bytes=1, state="held",
                             held_path="F:/Dashcam/Holding/camry/20260806/20260806_094436_NF.mp4")
        self._drive("20260806_094436_camry", "planned", ["20260806_094436_NF"])
        self.led.record_archived_clips("camry", ["20260721_131853_EF"])

        self.assertEqual(self.led.unsettled_stems("camry", ["20260806_094436_NF", "20260721_131853_EF"]), [])

    def test_one_unsettled_claim_is_enough(self):
        self._seen("20260806_094436_NF")
        self._drive("20260806_094436_camry", "ingested", ["20260806_094436_NF"])
        self._drive("20260806_094436_camry_other", "planned", ["20260806_094436_NF"])

        self.assertEqual(self.led.unsettled_stems("camry", ["20260806_094436_NF"]), ["20260806_094436_NF"])


class ReplanTests(unittest.TestCase):
    """A drive planned but never finished comes back on the next pass."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.led = Ledger(Path(self.tmp.name) / "state.sqlite")
        self.addCleanup(self.led.close)

    def test_only_unfinished_drives_contribute_their_clips(self):
        self.led.record_drive("camry", "20260806_094436_camry", status="planned")
        self.led.set_drive_clips("camry", "20260806_094436_camry", ["20260806_094436_NF", "20260806_094436_NR"])
        self.led.record_drive("camry", "20260806_124436_camry", status="ingested")
        self.led.set_drive_clips("camry", "20260806_124436_camry", ["20260806_124436_NF"])
        self.led.record_drive("civic", "20260806_094436_civic", status="planned")
        self.led.set_drive_clips("civic", "20260806_094436_civic", ["20260806_094436_NF"])

        stems = self.led.stems_of_drives("camry", "planned")

        self.assertEqual(stems, ["20260806_094436_NF", "20260806_094436_NR"])


class ImportMessageEraseTests(unittest.TestCase):
    """The load report also says whether the card can go."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.led = Ledger(Path(self.tmp.name) / "state.sqlite")
        self.addCleanup(self.led.close)

    def _message(self, **kwargs) -> str:
        notify.import_done(self.led, "camry", 5, 0, "batch", "http://viewer", recorded=2, **kwargs)
        return self.led.pending_messages()[-1]["message"]

    def test_a_card_with_nothing_outstanding_is_declared_safe(self):
        self.assertIn("safe to erase", self._message(card_safe=True))

    def test_a_card_with_something_outstanding_is_kept(self):
        self.assertIn("Keep the card", self._message(card_safe=False))

    def test_a_card_already_removed_gets_no_advice(self):
        body = self._message(card_safe=None)
        self.assertNotIn("safe to erase", body)
        self.assertNotIn("Keep the card", body)


if __name__ == "__main__":
    unittest.main()
