import struct
import threading
import unittest
from pathlib import Path

from metrology.errors import InputError
from metrology.ingress.binary import MAGIC
from metrology.ingress.directories import DirectoryDeferred
from metrology.ingress.transport import CollectionCancelled, iter_chunks
from metrology.service import Service
from gateway_support import make_frame, make_end, directory

MILLIS = 1794355200000  # 2026-11-11T00:00:00Z
RECEIVED = "2026-11-11T01:00:00Z"


def configured_service(path=":memory:", *, registered_versions=(1, 2)):
    app = Service(path)
    app.catalog.add_instrument(
        {"id": "probe1", "label": "Gateway probe", "channels": {"temperature": "degC"}}
    )
    app.catalog.add_policy(
        {"id": "temp", "unit": "degC", "lower": "-50", "upper": "100"}
    )
    app.catalog.bind("probe1", "temperature", "temp")
    app.catalog.add_certificate(
        {
            "id": "cert-temp",
            "instrument": "probe1",
            "channel": "temperature",
            "valid_from": "2026-01-01T00:00:00Z",
            "valid_to": "2027-01-01T00:00:00Z",
            "input_unit": "degC",
            "output_unit": "degC",
            "slope": "1",
            "intercept": "0",
            "uncertainty": "0.1",
            "minimum": "-100",
            "maximum": "200",
            "issued_at": "2025-12-01T00:00:00Z",
        }
    )
    # A second certificate window proves selection is driven by sampling time.
    app.catalog.add_certificate(
        {
            "id": "cert-temp-2027",
            "instrument": "probe1",
            "channel": "temperature",
            "valid_from": "2027-01-01T00:00:00Z",
            "valid_to": "2028-01-01T00:00:00Z",
            "input_unit": "degC",
            "output_unit": "degC",
            "slope": "2",
            "intercept": "0",
            "uncertainty": "0.1",
            "minimum": "-100",
            "maximum": "200",
            "issued_at": "2026-12-01T00:00:00Z",
        }
    )
    if 1 in registered_versions:
        app.directories.register(directory(1))
    if 2 in registered_versions:
        app.directories.register(
            directory(
                2,
                valid_from="2027-01-01T00:00:00Z",
                valid_until="2028-01-01T00:00:00Z",
            )
        )
    return app


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.app = configured_service()
        self.addCleanup(self.app.close)

    def collect(self, session, stream, **options):
        return self.app.gateway.collect(
            session, "gateway1", list(self._chunks(stream)),
            received_at=RECEIVED, **options
        )

    @staticmethod
    def _chunks(data, pattern=(1, 2, 3, 5, 7)):
        cursor = 0
        index = 0
        while cursor < len(data):
            size = pattern[index % len(pattern)]
            index += 1
            yield data[cursor : cursor + size]
            cursor += size

    def test_fixture_fault_switches_map_to_session_states(self):
        fixtures = Path(__file__).resolve().parents[1] / "fixtures"
        good = fixtures.joinpath("frames.bin").read_bytes()
        cut = fixtures.joinpath("frames.bin").read_bytes()[:-10]
        corrupt = fixtures.joinpath("corrupt.bin").read_bytes()
        good_status = self.collect("fix-good", good)
        cut_status = self.collect("fix-cut", cut)
        corrupt_status = self.collect("fix-bad", corrupt)
        self.assertEqual(good_status["state"], "complete")
        self.assertEqual(cut_status["state"], "truncated")
        self.assertEqual(corrupt_status["state"], "complete")
        self.assertTrue(corrupt_status["summary"]["missing"])

    def test_fixture_stream_ingests_observations_and_keeps_evidence(self):
        stream = Path(__file__).resolve().parents[1].joinpath(
            "fixtures", "frames.bin"
        ).read_bytes()
        status = self.collect("sess-1", stream)
        self.assertEqual(status["state"], "complete")
        summary = status["summary"]
        self.assertEqual(summary["frames_received"], 3)
        self.assertEqual(summary["unique_frames"], {"7:1": 2, "8:1": 1})
        self.assertEqual(summary["declared_counts"], {"7:1": 2, "8:1": 1})
        self.assertEqual(summary["missing"], {})
        frames = self.app.gateway.frames("sess-1")
        self.assertEqual([f["sequence"] for f in frames], [65535, 0, 0])
        self.assertEqual([f["generation"] for f in frames], [0, 1, 0])
        # Restart: the same gateway sequence number under a new boot is a new
        # source and a distinct observation.
        identifiers = {f["observation_id"] for f in frames}
        self.assertEqual(len(identifiers), 3)
        items = self.app.reports.query(limit=10)["items"]
        self.assertEqual(
            sorted(item["sequence"] for item in items), [0, 65535, 65536]
        )
        self.assertTrue(all(item["status"] == "accepted" for item in items))
        self.assertTrue(
            all(item["measurement"]["certificate"] == "cert-temp" for item in items)
        )
        self.assertTrue(self.app.inspector.check()["invariants"] == "ok")

    def test_wraparound_assigns_a_strictly_increasing_global_sequence(self):
        stream = b"".join(
            [
                make_frame(7, 65534, 1, 1, MILLIS, 10.0),
                make_frame(7, 65535, 1, 1, MILLIS, 11.0),
                make_frame(7, 0, 1, 1, MILLIS, 12.0),
                make_frame(7, 1, 1, 1, MILLIS, 13.0),
                make_end({"7:1": 4}),
            ]
        )
        self.collect("sess-wrap", stream)
        sequences = [
            item["sequence"]
            for item in self.app.reports.query(limit=10)["items"]
        ]
        self.assertEqual(sequences, [65534, 65535, 65536, 65537])

    def test_retransmission_is_counted_and_not_duplicated(self):
        frame = make_frame(7, 1, 1, 1, MILLIS, 10.0)
        stream = frame + frame + make_end({"7:1": 1})
        status = self.collect("sess-retry", stream)
        summary = status["summary"]
        self.assertEqual(summary["frames_received"], 2)
        self.assertEqual(summary["retransmissions"], 1)
        self.assertEqual(summary["unique_frames"], {"7:1": 1})
        self.assertEqual(self.app.reports.overview()["observations"], 1)
        stored = self.app.gateway.frames("sess-retry")
        self.assertEqual([f["status"] for f in stored], ["accepted", "retransmitted"])
        self.assertIsNone(stored[1]["observation_id"])

    def test_same_sequence_different_payload_is_a_conflict(self):
        first = make_frame(7, 1, 1, 1, MILLIS, 10.0)
        second = make_frame(7, 1, 1, 1, MILLIS, 99.0)
        stream = first + second + make_end({"7:1": 1})
        status = self.collect("sess-conflict", stream)
        self.assertEqual(status["summary"]["conflicts"], 1)
        self.assertEqual(status["batch"]["quarantined"], 1)
        stored = self.app.gateway.frames("sess-conflict")
        self.assertEqual([f["status"] for f in stored], ["accepted", "conflict"])
        quarantine = self.app.reports.batch("sess-conflict")["quarantines"]
        self.assertEqual(
            quarantine[0]["error"]["message"],
            "same sequence carries a different payload",
        )

    def test_crc_bad_frame_creates_a_reconcilable_gap(self):
        good = make_frame(7, 0, 1, 1, MILLIS, 10.0)
        bad = make_frame(7, 1, 1, 1, MILLIS, 11.0, corrupt=True)
        stream = good + bad + make_end({"7:1": 2})
        status = self.collect("sess-bad", stream)
        self.assertEqual(status["summary"]["frames_seen"], 2)
        self.assertEqual(status["summary"]["unique_frames"], {"7:1": 1})
        self.assertEqual(
            status["summary"]["missing"],
            {"7:1": {"declared": 2, "received": 1, "missing": 1}},
        )
        events = self.app.gateway.events("sess-bad")
        kinds = [event["kind"] for event in events]
        self.assertIn("crc_failure", kinds)
        crc_event = next(event for event in events if event["kind"] == "crc_failure")
        self.assertEqual(crc_event["evidence"]["bytes"], len(bad))

    def test_extra_frames_and_undeclared_channels_are_reconciled(self):
        frame_a = make_frame(7, 0, 1, 1, MILLIS, 10.0)
        frame_b = make_frame(7, 1, 1, 1, MILLIS, 11.0)
        undeclared = make_frame(8, 0, 1, 1, MILLIS, 12.0)
        # Trailer under-declares boot 7 and omits boot 8 entirely.
        stream = frame_a + frame_b + undeclared + make_end({"7:1": 1})
        status = self.collect("sess-extra", stream)
        self.assertEqual(
            status["summary"]["unexpected"],
            {
                "7:1": {"declared": 1, "received": 2, "extra": 1},
                "8:1": {"declared": 0, "received": 1, "extra": 1},
            },
        )
        self.assertEqual(status["summary"]["missing"], {})

    def test_truncated_and_corrupt_trailers_are_distinct_states(self):
        frame = make_frame(7, 0, 1, 1, MILLIS, 10.0)
        truncated = self.collect("sess-cut", frame)
        self.assertEqual(truncated["state"], "truncated")
        body = b"{broken"
        corrupt = frame + b"END!" + struct.pack(">H", len(body)) + body
        corrupt_status = self.collect("sess-corrupt-end", corrupt)
        self.assertEqual(corrupt_status["state"], "trailer_corrupt")
        self.assertEqual(corrupt_status["summary"]["trailer"], "corrupt")

    def test_deferred_frames_wait_for_their_directory_version(self):
        app = configured_service(registered_versions=(1,))
        self.addCleanup(app.close)
        frame_v2 = make_frame(7, 0, 1, 2, MILLIS, 10.0)
        status = app.gateway.collect(
            "sess-defer", "gateway1", [frame_v2 + make_end({"7:1": 1})],
            received_at=RECEIVED,
        )
        self.assertEqual(status["summary"]["deferred"], 1)
        self.assertEqual(status["batch"], None)
        deferred = app.gateway.frames("sess-defer", status="deferred")
        self.assertEqual(len(deferred), 1)
        # The current calibration must not be applied to an unknown directory.
        self.assertEqual(app.reports.overview()["observations"], 0)
        app.directories.register(directory(2))
        result = app.gateway.reprocess_session("sess-defer")
        self.assertEqual(result["reprocessed"], 1)
        self.assertEqual(result["batch"]["accepted"], 1)
        self.assertEqual(
            app.gateway.frames("sess-defer", status="ingested")[0]["observation_id"],
            "gw-gateway1-b7-c1-g0-s0",
        )

    def test_reprocessing_without_directory_still_defers(self):
        app = configured_service(registered_versions=(1,))
        self.addCleanup(app.close)
        frame = make_frame(7, 0, 1, 3, MILLIS, 10.0)
        app.gateway.collect(
            "sess-wait", "gateway1", [frame], received_at=RECEIVED
        )
        result = app.gateway.reprocess_session("sess-wait")
        self.assertEqual(result["reprocessed"], 0)
        self.assertEqual(result["still_deferred"], 1)
        self.assertIsNone(result["batch"])

    def test_repeated_reprocessing_uses_distinct_batches(self):
        app = configured_service(registered_versions=(1,))
        self.addCleanup(app.close)
        frame_v3 = make_frame(7, 0, 1, 3, MILLIS, 10.0)
        frame_v4 = make_frame(7, 1, 1, 4, MILLIS, 11.0)
        app.gateway.collect(
            "sess-multi", "gateway1", [frame_v3 + frame_v4], received_at=RECEIVED
        )
        self.assertEqual(
            len(app.gateway.frames("sess-multi", status="deferred")), 2
        )
        app.directories.register(directory(3))
        first = app.gateway.reprocess_session("sess-multi")
        self.assertEqual(first["reprocessed"], 1)
        self.assertEqual(first["still_deferred"], 1)
        self.assertEqual(first["batch"]["batch"], "sess-multi-reprocess")
        # A second call must not collide with the first reprocessing batch.
        app.directories.register(directory(4))
        second = app.gateway.reprocess_session("sess-multi")
        self.assertEqual(second["reprocessed"], 1)
        self.assertEqual(second["batch"]["batch"], "sess-multi-reprocess-2")
        self.assertEqual(
            app.gateway.frames("sess-multi", status="deferred"), []
        )

    def test_calibration_is_selected_by_sampling_time(self):
        millis_2027 = 1798761600000  # 2027-01-01T00:00:00Z
        frame = make_frame(7, 0, 1, 2, millis_2027, 10.0)
        self.collect("sess-2027", frame + make_end({"7:1": 1}))
        item = self.app.reports.query(limit=10)["items"][0]
        self.assertEqual(item["measurement"]["certificate"], "cert-temp-2027")
        # slope 2 doubles the raw reading.
        self.assertEqual(item["measurement"]["corrected_value"], "20")

    def test_out_of_order_frames_keep_reception_evidence(self):
        first = make_frame(7, 5, 1, 1, MILLIS, 10.0)
        second = make_frame(7, 3, 1, 1, MILLIS, 11.0)
        self.collect("sess-ooo", first + second)
        events = [
            event["kind"] for event in self.app.gateway.events("sess-ooo")
        ]
        self.assertIn("out_of_order", events)
        # Frames remain stored in true reception order.
        stored = self.app.gateway.frames("sess-ooo")
        self.assertEqual([f["sequence"] for f in stored], [5, 3])

    def test_cancellation_closes_the_session_and_keeps_received_frames(self):
        frame = make_frame(7, 0, 1, 1, MILLIS, 10.0)
        cancelled = threading.Event()

        def chunks():
            yield frame
            cancelled.set()
            yield frame

        status = self.app.gateway.collect(
            "sess-cancel", "gateway1", chunks(),
            received_at=RECEIVED, cancelled=cancelled.is_set,
        )
        self.assertEqual(status["state"], "cancelled")
        self.assertEqual(status["summary"]["frames_received"], 1)

    def test_deferred_cache_is_bounded(self):
        app = configured_service(registered_versions=())
        self.addCleanup(app.close)
        app.gateway.max_pending_frames = 2
        stream = b"".join(
            make_frame(7, seq, 1, 9, MILLIS, float(seq)) for seq in range(4)
        )
        with self.assertRaises(InputError):
            app.gateway.collect(
                "sess-bound", "gateway1", [stream], received_at=RECEIVED
            )
        status = app.gateway.status("sess-bound")
        self.assertEqual(status["state"], "failed")
        # Frames received up to the bound remain as reception evidence; the
        # bound trips while handling the third deferred frame.
        self.assertEqual(len(app.gateway.frames("sess-bound")), 3)
        self.assertEqual(app.reports.overview()["observations"], 0)

    def test_isolated_frame_reprocessing(self):
        frame = make_frame(7, 4, 1, 1, MILLIS, 15.0)
        result = self.app.gateway.reprocess_frame(
            "gateway1", frame, batch_id="iso-1", received_at=RECEIVED
        )
        self.assertEqual(result["accepted"], 1)
        with self.assertRaises(InputError):
            self.app.gateway.reprocess_frame(
                "gateway1", frame + frame, batch_id="iso-2", received_at=RECEIVED
            )
        with self.assertRaises(InputError):
            self.app.gateway.reprocess_frame(
                "gateway1", b"\x00\x00", batch_id="iso-3", received_at=RECEIVED
            )

    def test_session_id_is_unique(self):
        frame = make_frame(7, 0, 1, 1, MILLIS, 10.0)
        self.collect("sess-once", frame)
        from metrology.errors import Conflict

        with self.assertRaises(Conflict):
            self.collect("sess-once", frame)


class TransportTests(unittest.TestCase):
    def test_loopback_stream_respects_cancellation(self):
        import sys

        support_root = str(Path(__file__).resolve().parents[1] / "demo_support")
        if support_root not in sys.path:
            sys.path.insert(0, support_root)
        from source import start

        server, base = start()
        try:
            chunks = list(iter_chunks(base + "/frames", chunk_size=7))
            self.assertTrue(all(len(chunk) <= 7 for chunk in chunks))
            parsed = b"".join(chunks)
            self.assertTrue(parsed.startswith(MAGIC))

            cancelled = threading.Event()
            cancelled.set()
            with self.assertRaises(CollectionCancelled):
                list(iter_chunks(base + "/frames", cancelled=cancelled.is_set))
        finally:
            server.shutdown()
            server.server_close()

    def test_non_http_url_is_rejected(self):
        with self.assertRaises(InputError):
            list(iter_chunks("ftp://127.0.0.1:9/frames"))


if __name__ == "__main__":
    unittest.main()
