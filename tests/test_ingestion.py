import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from metrology.errors import InputError, Conflict, IntegrityError
from metrology.ingress.readers import (
    read_jsonl,
    read_csv,
    InputRow,
    load_text,
    summarize,
)
from metrology.ingress.mapping import Mapping
from metrology.ingress.delivery import DeliveryManifest, DeliveryInbox
from support import application, observation, rows, RECEIVED


def mapping():
    return Mapping.parse(
        {
            "name": "bench",
            "columns": {
                "id": "reading",
                "sequence": "offset",
                "event_time": "at",
                "value": "pressure",
            },
            "constants": {
                "source": "s1",
                "instrument": "i1",
                "channel": "p",
                "unit": "kPa",
            },
            "tags": {"lot": "lot"},
        }
    )


class ParsingTests(unittest.TestCase):
    def test_jsonl_quarantines_individual_bad_lines(self):
        text = (
            json.dumps(observation().as_dict())
            + "\n{bad}\n\n"
            + json.dumps(observation(1).as_dict())
        )
        result = read_jsonl(text)
        self.assertEqual(len(result), 3)
        self.assertTrue(result[0].valid)
        self.assertFalse(result[1].valid)
        self.assertEqual(result[1].line, 2)
        self.assertEqual(result[2].line, 4)
        report = summarize(result)
        self.assertEqual(report["valid"], 2)
        self.assertEqual(report["invalid"], 1)
        self.assertEqual(report["sources"], {"s1": 2})

    def test_jsonl_limits_are_explicit(self):
        text = json.dumps(observation().as_dict())
        with self.assertRaises(InputError):
            read_jsonl(text + "\n" + text, maximum_rows=1)
        result = read_jsonl(text, maximum_line=10)
        self.assertEqual(len(result), 1)
        self.assertFalse(result[0].valid)
        self.assertEqual(result[0].error.code, "invalid_input")
        result = read_jsonl("[1,2,3]")
        self.assertFalse(result[0].valid)

    def test_csv_mapping_and_quoted_fields(self):
        text = 'reading,offset,at,pressure,lot\nr0,0,2026-11-11T00:00:00Z,10,"A,B"\n'
        result = read_csv(text, mapping())
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0].valid)
        self.assertEqual(result[0].observation.tags["lot"], "A,B")
        self.assertEqual(result[0].observation.sequence, 0)
        self.assertEqual(result[0].observation.value, 10)

    def test_csv_width_and_duplicate_headers(self):
        text = "reading,offset,at,pressure,lot\nr0,0,2026-11-11T00:00:00Z,10\n"
        result = read_csv(text, mapping())
        self.assertFalse(result[0].valid)
        with self.assertRaises(InputError):
            read_csv("reading,reading,offset,at,pressure,lot\n", mapping())
        with self.assertRaises(InputError):
            read_csv("", mapping())
        with self.assertRaises(InputError):
            read_csv("reading,offset\n", mapping())
        with self.assertRaises(InputError):
            read_csv(text, None)

    def test_comma_decimal_is_deliberate(self):
        data = mapping().as_dict()
        data["decimal_separator"] = ","
        comma = Mapping.parse(data)
        text = "reading;offset;at;pressure;lot\nr0;0;2026-11-11T00:00:00Z;10,5;A\n"
        result = read_csv(text, comma, delimiter=";")
        self.assertTrue(result[0].valid)
        self.assertEqual(str(result[0].observation.value), "10.5")
        mixed = text.replace("10,5", "10.5")
        self.assertFalse(read_csv(mixed, comma, delimiter=";")[0].valid)

    def test_mapping_contract_is_complete(self):
        data = mapping().as_dict()
        self.assertEqual(Mapping.parse(data).as_dict(), data)
        with self.assertRaises(InputError):
            Mapping.parse({**data, "constants": {**data["constants"], "id": "x"}})
        with self.assertRaises(InputError):
            Mapping.parse({**data, "columns": {"value": "v"}})
        with self.assertRaises(InputError):
            Mapping.parse({**data, "columns": {**data["columns"], "bogus": "v"}})
        record = {
            "reading": "r",
            "offset": "1.2",
            "at": "2026-11-11T00:00:00Z",
            "pressure": "10",
            "lot": "A",
        }
        with self.assertRaises(InputError):
            mapping().apply(record)
        del record["lot"]
        record["offset"] = "1"
        with self.assertRaises(InputError):
            mapping().apply(record)

    def test_file_encoding_and_links(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "data.txt"
            path.write_bytes(b"\xef\xbb\xbfhello")
            self.assertEqual(load_text(path), "hello")
            with self.assertRaises(InputError):
                load_text(path, maximum_bytes=3)
            path.write_bytes(b"\xff")
            with self.assertRaises(InputError):
                load_text(path)
            link = Path(directory) / "link"
            link.symlink_to(path)
            with self.assertRaises(InputError):
                load_text(link)


class BatchTests(unittest.TestCase):
    def setUp(self):
        self.app = application()
        self.addCleanup(self.app.close)

    def test_batch_retry_and_duplicate_observation(self):
        values = rows(observation(), observation(1))
        result = self.app.pipeline.ingest("b1", values, received_at=RECEIVED)
        self.assertEqual(result["accepted"], 2)
        replay = self.app.pipeline.ingest("b1", values, received_at=RECEIVED)
        self.assertTrue(replay["reused"])
        second = self.app.pipeline.ingest("b2", values, received_at=RECEIVED)
        self.assertEqual(second["duplicates"], 2)
        self.assertEqual(second["inserted"], 0)
        self.assertEqual(self.app.reports.overview()["observations"], 2)
        self.assertEqual(self.app.inspector.check()["invariants"], "ok")

    def test_batch_id_conflict_is_atomic(self):
        self.app.pipeline.ingest("b1", rows(observation()), received_at=RECEIVED)
        before = self.app.inspector.manifest()
        with self.assertRaises(Conflict):
            self.app.pipeline.ingest("b1", rows(observation(1)), received_at=RECEIVED)
        self.assertTrue(self.app.inspector.compare_manifest(before)["same"])
        with self.assertRaises(Conflict):
            self.app.pipeline.ingest(
                "b1", rows(observation()), received_at="2026-11-12T00:00:00Z"
            )
        self.assertEqual(self.app.reports.overview()["batches"], 1)

    def test_conflicting_id_rolls_back_earlier_row(self):
        self.app.pipeline.ingest("b1", rows(observation(5)), received_at=RECEIVED)
        with self.assertRaises(Conflict):
            self.app.pipeline.ingest(
                "b2",
                rows(observation(0), observation(5, value="11")),
                received_at=RECEIVED,
            )
        self.assertEqual(self.app.reports.overview()["observations"], 1)
        self.assertEqual(self.app.reports.overview()["batches"], 1)
        self.assertEqual(self.app.reports.sequence_gaps("s1")["missing"], 5)
        self.assertEqual(self.app.inspector.check()["invariants"], "ok")

    def test_sequence_collision_is_not_silently_deduplicated(self):
        self.app.pipeline.ingest("b1", rows(observation()), received_at=RECEIVED)
        with self.assertRaises(Conflict):
            self.app.pipeline.ingest(
                "b2", rows(observation(id="different")), received_at=RECEIVED
            )
        self.assertEqual(self.app.reports.overview()["observations"], 1)
        self.assertEqual(self.app.pipeline.get("r0")["raw"]["value"], "10")

    def test_atomic_quality_failure_leaves_no_partial_state(self):
        before = self.app.inspector.manifest()
        with self.assertRaises(InputError):
            self.app.pipeline.ingest(
                "b1",
                rows(observation(), observation(1, value="30")),
                received_at=RECEIVED,
                atomic=True,
            )
        self.assertTrue(self.app.inspector.compare_manifest(before)["same"])
        self.assertEqual(self.app.catalog.source("s1")["high_sequence"], -1)
        self.assertEqual(self.app.reports.overview()["batches"], 0)

    def test_partial_quality_and_parse_failures_are_visible(self):
        values = rows(
            observation(), observation(1, value="30"), observation(2, value="101")
        )
        values.append(InputRow(4, error=InputError("malformed synthetic row")))
        result = self.app.pipeline.ingest("b1", values, received_at=RECEIVED)
        self.assertEqual(result["inserted"], 3)
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["quarantined"], 2)
        self.assertEqual(
            self.app.pipeline.get("r2")["error"]["code"], "calibration_error"
        )
        self.assertFalse(self.app.pipeline.get("r1")["measurement"]["accepted"])
        report = self.app.reports.batch("b1")
        self.assertEqual(report["quarantines"][0]["line"], 4)
        self.assertEqual(self.app.inspector.check()["invariants"], "ok")

    def test_unknown_source_is_retained_as_error(self):
        result = self.app.pipeline.ingest(
            "b1", rows(observation(source="missing")), received_at=RECEIVED
        )
        self.assertEqual(result["quarantined"], 1)
        self.assertEqual(self.app.pipeline.get("r0")["error"]["code"], "not_found")
        self.app.catalog.add_source("missing")
        report = self.app.pipeline.replay(
            "repair", "i1", "p", "2026-11-11T00:00:00Z", "2026-11-11T00:01:00Z"
        )
        self.assertEqual(report["accepted"], 1)
        self.assertEqual(self.app.pipeline.get("r0")["revision"], 2)

    def test_lateness_boundary_is_half_open(self):
        self.app.catalog.watermark("s1", "2026-11-11T00:00:10Z")
        result = self.app.pipeline.ingest(
            "b1", rows(observation(9), observation(10)), received_at=RECEIVED
        )
        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["accepted"], 1)
        rejected = self.app.pipeline.get("r9")["measurement"]
        self.assertIn("late", [issue["code"] for issue in rejected["issues"]])
        self.assertTrue(self.app.pipeline.get("r10")["measurement"]["accepted"])
        with self.assertRaises(Conflict):
            self.app.catalog.watermark("s1", "2026-11-11T00:00:09Z")

    def test_batch_sorts_event_time_before_step_checks(self):
        self.app.catalog.add_policy({"id": "q2", "unit": "kPa", "maximum_step": "2"})
        self.app.catalog.bind("i1", "p", "q2")
        result = self.app.pipeline.ingest(
            "b1",
            rows(
                observation(2, value="20"),
                observation(0, value="10"),
                observation(1, value="11"),
            ),
            received_at=RECEIVED,
        )
        self.assertEqual(result["accepted"], 2)
        self.assertEqual(result["rejected"], 1)
        self.assertEqual(
            self.app.pipeline.get("r2")["measurement"]["issues"][0]["code"], "step"
        )
        self.assertEqual(self.app.pipeline.get("r1")["measurement"]["issues"], [])

    def test_duplicate_positions_and_empty_batches_rejected(self):
        with self.assertRaises(InputError):
            self.app.pipeline.ingest("b1", [], received_at=RECEIVED)
        with self.assertRaises(InputError):
            self.app.pipeline.ingest(
                "b1",
                [InputRow(1, observation()), InputRow(1, observation(1))],
                received_at=RECEIVED,
            )
        with self.assertRaises(InputError):
            self.app.pipeline.ingest("b1", [observation()], received_at=RECEIVED)
        self.assertEqual(self.app.reports.overview()["observations"], 0)


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.app = application()
        self.addCleanup(self.app.close)
        self.inbox = DeliveryInbox(self.app.database, self.app.pipeline)

    def manifest(self):
        files = []
        for index in range(2):
            payload = (json.dumps(observation(index).as_dict()) + "\n").encode()
            name = f"part{index}.jsonl"
            (self.root / name).write_bytes(payload)
            files.append(
                {
                    "path": name,
                    "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "format": "jsonl",
                    "batch": f"b{index}",
                }
            )
        return {"id": "delivery", "received_at": RECEIVED, "files": files}

    def test_delivery_resume_commits_files_once(self):
        manifest = self.manifest()
        first = self.inbox.receive(manifest, self.root, maximum_files=1)
        self.assertEqual(first["state"], "pending")
        self.assertEqual(first["committed_files"], 1)
        self.assertEqual(len(self.inbox.pending()), 1)
        second = self.inbox.receive(manifest, self.root)
        self.assertEqual(second["state"], "complete")
        self.assertEqual(second["committed_files"], 2)
        self.assertEqual(self.inbox.pending(), [])
        third = self.inbox.receive(manifest, self.root)
        self.assertTrue(third["reused"])
        self.assertEqual(self.app.reports.overview()["observations"], 2)
        self.assertEqual(self.app.inspector.check()["invariants"], "ok")

    def test_checksum_failure_prevents_any_ingestion(self):
        manifest = self.manifest()
        (self.root / "part1.jsonl").write_text("changed")
        with self.assertRaises(IntegrityError):
            self.inbox.receive(manifest, self.root)
        self.assertEqual(self.app.reports.overview()["batches"], 0)
        self.assertEqual(self.inbox.pending(), [])

    def test_paths_cannot_escape_inbox(self):
        manifest = self.manifest()
        for path in ("../file", "/tmp/file", "a//b", "a/./b", "a\\b"):
            changed = {**manifest, "files": [{**manifest["files"][0], "path": path}]}
            with self.subTest(path=path):
                with self.assertRaises(InputError):
                    DeliveryManifest(changed)
        (self.root / "part0.jsonl").unlink()
        (self.root / "part0.jsonl").symlink_to(self.root / "part1.jsonl")
        with self.assertRaises(InputError):
            self.inbox.receive(manifest, self.root)

    def test_delivery_identity_is_frozen(self):
        manifest = self.manifest()
        self.inbox.receive(manifest, self.root, maximum_files=1)
        changed = {**manifest, "received_at": "2026-11-12T00:00:00Z"}
        with self.assertRaises(Conflict):
            self.inbox.receive(changed, self.root)
        status = self.inbox.status("delivery")
        self.assertEqual(status["state"], "pending")
        self.assertEqual(status["files"][0]["state"], "complete")
        self.assertEqual(status["files"][1]["state"], "pending")
