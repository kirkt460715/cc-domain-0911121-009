import struct
import unittest
import zlib
from pathlib import Path

from metrology.ingress.binary import (
    FrameParser,
    MAGIC,
    END_MARKER,
    MAX_FRAME_BYTES,
    decode_payload,
)
from gateway_support import make_frame, make_end, chunked, cycling_sizes

MILLIS = 1794355200000  # 2026-11-11T00:00:00Z


class ParserTests(unittest.TestCase):
    def test_fixture_frames_parse_under_arbitrary_cuts(self):
        data = Path(__file__).resolve().parents[1].joinpath(
            "fixtures", "frames.bin"
        ).read_bytes()
        for pattern in ((1,), (2, 3), (5, 1, 7), (34, 1, 38, 2)):
            with self.subTest(pattern=pattern):
                parser = FrameParser()
                events = []
                for chunk in chunked(data, cycling_sizes(iter(pattern))):
                    events.extend(parser.feed(chunk))
                events.extend(parser.finish())
                kinds = [event.kind for event in events]
                self.assertEqual(kinds, ["frame", "frame", "frame", "end_marker"])
                sequences = [
                    event.evidence["frame"].payload.sequence
                    for event in events[:-1]
                ]
                self.assertEqual(sequences, [65535, 0, 0])
                self.assertEqual(parser.counts, {(7, 1): 2, (8, 1): 1})

    def test_crc_failure_resyncs_at_next_magic(self):
        good_a = make_frame(7, 10, 1, 1, MILLIS, 1.0)
        bad = make_frame(7, 11, 1, 1, MILLIS, 2.0, corrupt=True)
        good_b = make_frame(7, 12, 1, 1, MILLIS, 3.0)
        stream = good_a + bad + good_b
        events = list(parse_process(stream))
        self.assertEqual([e.kind for e in events], ["frame", "crc_failure", "frame"])
        failure = events[1]
        self.assertEqual(failure.offset, len(good_a))
        self.assertEqual(failure.evidence["bytes"], len(bad))
        # The following frame is recovered intact despite the bad frame.
        self.assertEqual(events[2].evidence["frame"].payload.sequence, 12)

    def test_magic_inside_gap_is_not_half_frame_confusion(self):
        # Magic bytes in noise start a candidate frame; the implausible length
        # is rejected and the parser resynchronizes onto the real frame.
        frame = make_frame(7, 1, 1, 1, MILLIS, 1.0)
        noise = b"\x00" + MAGIC + b"\x10\x00"
        parser = FrameParser()
        events = parser.feed(noise + frame) + parser.finish()
        self.assertEqual(events[0].kind, "resync_gap")
        self.assertEqual(events[0].evidence["bytes"], 1)
        self.assertEqual(events[1].kind, "frame_too_long")
        final = [event for event in events if event.kind == "frame"]
        self.assertEqual(len(final), 1)
        self.assertEqual(final[0].evidence["frame"].offset, len(noise))

    def test_gap_offsets_cover_every_byte(self):
        frame_a = make_frame(7, 1, 1, 1, MILLIS, 1.0)
        frame_b = make_frame(7, 2, 1, 1, MILLIS, 2.0)
        gap = b"\x99\x88\x77"
        parser = FrameParser()
        events = parser.feed(frame_a + gap + frame_b) + parser.finish()
        gap_events = [event for event in events if event.kind == "resync_gap"]
        self.assertEqual(len(gap_events), 1)
        self.assertEqual(gap_events[0].offset, len(frame_a))
        self.assertEqual(gap_events[0].evidence["bytes"], len(gap))
        self.assertEqual(events[-1].evidence["frame"].offset, len(frame_a) + len(gap))

    def test_gap_bytes_are_never_silently_dropped(self):
        # Whatever the chunking, every non-frame byte appears in one gap event.
        frame_a = make_frame(7, 1, 1, 1, MILLIS, 1.0)
        frame_b = make_frame(7, 2, 1, 1, MILLIS, 2.0)
        gap = b"\x99\x88\x77"
        stream = frame_a + gap + frame_b
        for pattern in ((1,), (2, 3), (5, 1, 7)):
            parser = FrameParser()
            events = []
            for chunk in chunked(stream, cycling_sizes(pattern)):
                events.extend(parser.feed(chunk))
            events.extend(parser.finish())
            accounted = sum(
                event.evidence["bytes"]
                for event in events
                if event.kind == "resync_gap"
            )
            self.assertEqual(accounted, len(gap), pattern)

    def test_declared_length_above_limit_is_reported_and_skipped(self):
        oversized = MAGIC + struct.pack(">H", MAX_FRAME_BYTES - 3)
        frame = make_frame(7, 1, 1, 1, MILLIS, 1.0)
        events = list(parse_process(oversized + frame))
        self.assertEqual(events[0].kind, "frame_too_long")
        self.assertEqual(events[0].evidence["maximum"], MAX_FRAME_BYTES)
        self.assertEqual(events[1].kind, "frame")

    def test_truncated_stream_is_distinct_from_complete_end(self):
        frame = make_frame(7, 1, 1, 1, MILLIS, 1.0)
        truncated = frame[:-3]
        events = list(parse_process(truncated))
        self.assertTrue(any(e.kind == "resync_gap" for e in events))
        parser = FrameParser()
        list(parser.feed(truncated))
        events = parser.finish()
        self.assertEqual(events[0].kind, "resync_gap")
        self.assertIsNone(parser.counts)

    def test_bad_end_marker_is_reported(self):
        body = b"{not json"
        marker = END_MARKER + struct.pack(">H", len(body)) + body
        events = list(parse_process(marker))
        self.assertEqual([e.kind for e in events], ["bad_end_marker"])

    def test_end_marker_split_across_chunks(self):
        marker = make_end({"7:1": 1})
        parser = FrameParser()
        events = []
        for cut in range(1, len(marker)):
            parser = FrameParser()
            events = parser.feed(marker[:cut]) + parser.feed(marker[cut:])
            events += parser.finish()
            self.assertEqual([e.kind for e in events], ["end_marker"], cut)
            self.assertEqual(parser.counts, {(7, 1): 1})

    def test_non_finite_payload_is_an_event_not_a_frame(self):
        frame = make_frame(7, 1, 1, 1, MILLIS, float("nan"))
        events = list(parse_process(frame))
        self.assertEqual([e.kind for e in events], ["invalid_payload"])

    def test_unsupported_payload_length_is_reported(self):
        payload = b"\x00" * 10
        head = MAGIC + struct.pack(">H", len(payload))
        crc = struct.pack(">I", zlib.crc32(head + payload) & 0xFFFFFFFF)
        events = list(parse_process(head + payload + crc))
        self.assertEqual([e.kind for e in events], ["unsupported_payload"])

    def test_buffer_bound_forces_a_bounded_gap(self):
        bound = MAX_FRAME_BYTES + 32
        parser = FrameParser(max_buffered_bytes=bound)
        events = parser.feed(b"\x00" * (bound * 3))
        self.assertTrue(events)
        self.assertTrue(all(e.kind == "resync_gap" for e in events))
        total = sum(e.evidence["bytes"] for e in events)
        self.assertEqual(total, bound * 3 - len(parser._buffer))
        self.assertLessEqual(len(parser._buffer), bound)
        # A following real frame is still parsed after the forced gaps.
        frame = make_frame(7, 1, 1, 1, MILLIS, 1.0)
        events = parser.feed(frame) + parser.finish()
        self.assertEqual(events[0].kind, "frame")
        self.assertEqual(
            events[0].evidence["frame"].payload.sequence, 1
        )

    def test_decode_payload_rejects_out_of_range_time(self):
        payload, problem = decode_payload(b"\x00" * 8, 0)
        self.assertIsNone(payload)
        self.assertEqual(problem.kind, "unsupported_payload")


def parse_process(data):
    parser = FrameParser()
    events = []
    for i in range(0, len(data), 7):
        events.extend(parser.feed(data[i : i + 7]))
    events.extend(parser.finish())
    return events


if __name__ == "__main__":
    unittest.main()
