import io
import json
import tempfile
import threading
import unittest
from pathlib import Path
from contextlib import redirect_stdout, redirect_stderr
from metrology.service import Service
from metrology.errors import Conflict, StateError, Missing, InputError, IntegrityError
from metrology.output.snapshots import verify_export
from metrology.output.bundles import verify_bundle
from metrology.processing.windows import WindowSpec
from metrology.processing.diagnostics import percentile, robust_summary, Diagnostics
from metrology.console import main, dispatch
from support import (
    application,
    observation,
    rows,
    certificate,
    window,
    published,
    START,
    END,
    RECEIVED,
)


class WindowLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.app = application()
        self.addCleanup(self.app.close)

    def test_window_statistics_and_half_open_boundaries(self):
        self.app.pipeline.ingest(
            "b",
            rows(
                observation(0, value="10"),
                observation(30, value="12"),
                observation(60, event_time=END, value="19"),
            ),
            received_at=RECEIVED,
        )
        result = self.app.windows.materialize(window())
        item = result["windows"][0]
        self.assertEqual(item["statistics"]["count"], 2)
        self.assertEqual(item["statistics"]["mean"], "11")
        self.assertEqual(item["statistics"]["stddev"], "1")
        self.assertEqual(item["accepted"], ["r0", "r30"])
        self.assertEqual(item["state"], "open")
        with self.assertRaises(StateError):
            self.app.windows.seal("w1", 1)
        self.app.catalog.watermark("s1", END)
        final = self.app.windows.materialize(window(), 1)
        self.assertEqual(final["generation"], 2)
        self.assertEqual(final["windows"][0]["state"], "final")
        sealed = self.app.windows.seal("w1", 2)
        self.assertTrue(sealed["sealed"])
        self.assertEqual(self.app.windows.seal("w1", 2), sealed)
        with self.assertRaises(StateError):
            self.app.windows.materialize(window())

    def test_sparse_windows_cannot_be_sealed(self):
        self.app.catalog.watermark("s1", END)
        result = self.app.windows.materialize(window(seconds=30))
        self.assertEqual(len(result["windows"]), 2)
        self.assertEqual(result["windows"][0]["statistics"]["count"], 0)
        self.assertIsNone(result["windows"][0]["statistics"]["mean"])
        self.assertFalse(result["windows"][0]["sufficient"])
        with self.assertRaises(StateError):
            self.app.windows.seal("w1", 1)
        self.assertFalse(self.app.windows.get("w1")["sealed"])

    def test_all_sources_must_cross_lateness_boundary(self):
        self.app.catalog.add_source("s2", 10)
        self.app.pipeline.ingest("b", rows(observation()), received_at=RECEIVED)
        self.app.catalog.watermark("s1", END)
        self.app.catalog.watermark("s2", END)
        spec = window(sources=["s1", "s2"])
        first = self.app.windows.materialize(spec)
        self.assertEqual(first["windows"][0]["state"], "open")
        self.app.catalog.watermark("s2", "2026-11-11T00:01:10Z")
        second = self.app.windows.materialize(spec)
        self.assertEqual(second["windows"][0]["state"], "final")
        self.assertEqual(second["windows"][0]["statistics"]["count"], 1)

    def test_stale_generations_and_lineage_prevent_seal(self):
        self.app.pipeline.ingest("b", rows(observation()), received_at=RECEIVED)
        self.app.catalog.watermark("s1", END)
        self.app.windows.materialize(window())
        with self.assertRaises(Conflict):
            self.app.windows.materialize(window(), 99)
        self.app.pipeline.replay("repair", "i1", "p", START, END)
        with self.assertRaises(Conflict):
            self.app.windows.seal("w1", 1)
        stale = self.app.inspector.stale_windows()
        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0]["stale"][0]["current_revision"], 2)
        self.app.windows.materialize(window(), 1)
        self.app.windows.seal("w1", 2)
        self.assertEqual(self.app.inspector.stale_windows(), [])

    def test_window_definition_cannot_change(self):
        self.app.windows.materialize(window())
        with self.assertRaises(Conflict):
            self.app.windows.materialize(window(seconds=30))
        with self.assertRaises(InputError):
            WindowSpec.parse(window(seconds=7))
        with self.assertRaises(InputError):
            WindowSpec.parse(window(sources=["s1", "s1"]))
        with self.assertRaises(InputError):
            WindowSpec.parse(window(end=START))
        with self.assertRaises(Missing):
            self.app.windows.get("missing")


class ReplayAndSnapshotsTests(unittest.TestCase):
    def setUp(self):
        self.app = application()
        self.addCleanup(self.app.close)

    def test_replay_preserves_old_results_and_snapshot(self):
        published(self.app)
        before = self.app.snapshots.get("snapshot")
        self.app.catalog.revoke_certificate("c1", "replaced by corrected coefficient")
        self.app.catalog.add_certificate(certificate(id="c2", intercept="1"))
        result = self.app.pipeline.replay("recalibrate", "i1", "p", START, END)
        self.assertEqual(result["changed"], 1)
        self.assertEqual(
            self.app.pipeline.get("r0")["measurement"]["corrected_value"], "11"
        )
        self.assertEqual(
            self.app.pipeline.get("r0", 1)["measurement"]["corrected_value"], "10"
        )
        self.assertEqual(self.app.snapshots.get("snapshot"), before)
        history = self.app.pipeline.history("r0")
        self.assertEqual([item["revision"] for item in history], [1, 2])
        self.assertFalse(history[0]["active"])
        self.assertTrue(history[1]["active"])
        self.assertTrue(
            self.app.pipeline.replay("recalibrate", "i1", "p", START, END)["reused"]
        )
        self.assertEqual(self.app.inspector.check()["invariants"], "ok")

    def test_replay_optimistic_conflict_rolls_back(self):
        self.app.pipeline.ingest(
            "b", rows(observation(), observation(1)), received_at=RECEIVED
        )
        with self.assertRaises(Conflict):
            self.app.pipeline.replay(
                "repair", "i1", "p", START, END, expected_revisions={"r0": 1, "r1": 9}
            )
        self.assertEqual(self.app.pipeline.get("r0")["revision"], 1)
        self.assertEqual(self.app.pipeline.get("r1")["revision"], 1)
        self.assertEqual(self.app.reports.overview()["replays"], 0)
        with self.assertRaises(Conflict):
            self.app.pipeline.replay(
                "repair", "i1", "p", START, END, expected_revisions={"outside": 1}
            )

    def test_snapshot_export_is_deterministic_and_no_clobber(self):
        published(self.app)
        with tempfile.TemporaryDirectory() as directory:
            first, second = Path(directory) / "one.json", Path(directory) / "two.json"
            self.app.snapshots.export("snapshot", first)
            self.app.snapshots.export("snapshot", second)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            verified = verify_export(first)
            self.assertEqual(verified["observations"], 1)
            self.assertEqual(verified["id"], "snapshot")
            with self.assertRaises(Conflict):
                self.app.snapshots.export("snapshot", first)
            data = json.loads(first.read_text())
            data["payload"]["id"] = "tampered"
            first.write_text(json.dumps(data))
            with self.assertRaises(IntegrityError):
                verify_export(first)
        self.assertIn(
            "window_set,generation,start,end", self.app.snapshots.csv("snapshot")
        )
        self.assertTrue(self.app.snapshots.create("snapshot", ["w1"])["reused"])

    def test_compare_snapshots_tracks_revisions(self):
        published(self.app)
        self.app.pipeline.replay("repair", "i1", "p", START, END)
        self.app.windows.materialize(window(id="w2"))
        self.app.windows.seal("w2", 1)
        self.app.snapshots.create("second", ["w2"])
        diff = self.app.snapshots.compare("snapshot", "second")
        self.assertEqual(diff["added"], [])
        self.assertEqual(diff["removed"], [])
        self.assertEqual(
            diff["changed"], [{"id": "r0", "before_revision": 1, "after_revision": 2}]
        )
        with self.assertRaises(Conflict):
            self.app.snapshots.create("snapshot", ["w2"])

    def test_replay_plan_detects_changed_selection(self):
        self.app.pipeline.ingest("b", rows(observation()), received_at=RECEIVED)
        plan = self.app.replay_plans.plan("plan", "i1", "p", START, END)
        self.assertEqual(plan["state"], "planned")
        self.assertEqual(plan["preview"]["expected_revisions"], {"r0": 1})
        self.app.pipeline.ingest("b2", rows(observation(1)), received_at=RECEIVED)
        with self.assertRaises(Conflict):
            self.app.replay_plans.execute("plan")
        self.assertEqual(self.app.pipeline.get("r0")["revision"], 1)
        self.assertEqual(self.app.replay_plans.get("plan")["state"], "planned")

    def test_replay_plan_executes_once_and_detects_policy_change(self):
        self.app.pipeline.ingest("b", rows(observation()), received_at=RECEIVED)
        self.app.replay_plans.plan("plan", "i1", "p", START, END)
        result = self.app.replay_plans.execute("plan")
        self.assertEqual(result["state"], "executed")
        self.assertEqual(self.app.replay_plans.execute("plan"), result)
        self.assertEqual(self.app.pipeline.get("r0")["revision"], 2)
        self.app.replay_plans.plan("next", "i1", "p", START, END)
        self.app.catalog.add_policy({"id": "q2", "unit": "kPa", "upper": "5"})
        self.app.catalog.bind("i1", "p", "q2")
        with self.assertRaises(Conflict):
            self.app.replay_plans.execute("next")
        self.assertEqual(self.app.pipeline.get("r0")["revision"], 2)


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.app = application()
        self.addCleanup(self.app.close)
        self.plan = {
            "id": "campaign",
            "start": START,
            "end": END,
            "lot": "A",
            "channels": [
                {
                    "instrument": "i1",
                    "channel": "p",
                    "minimum_count": 2,
                    "maximum_gap_seconds": 40,
                    "sources": ["s1"],
                }
            ],
        }

    def prepare(self):
        self.app.campaigns.plan(self.plan)
        self.app.campaigns.start("campaign", 1)
        self.app.pipeline.ingest(
            "b", rows(observation(), observation(30)), received_at=RECEIVED
        )
        self.app.catalog.watermark("s1", END)
        self.app.campaigns.submit("campaign", 2)

    def test_campaign_requires_complete_sampling_and_watermark(self):
        first = self.app.campaigns.plan(self.plan)
        self.assertEqual(first["state"], "planned")
        self.assertEqual(self.app.campaigns.plan(self.plan), first)
        self.app.campaigns.start("campaign", 1)
        assessed = self.app.campaigns.assess("campaign")
        self.assertFalse(assessed["ready"])
        self.assertIn("sources_not_final", assessed["checks"][0]["issues"])
        self.assertIn("insufficient_count", assessed["checks"][0]["issues"])
        with self.assertRaises(StateError):
            self.app.campaigns.submit("campaign", 2)
        self.assertEqual(self.app.campaigns.get("campaign")["state"], "collecting")

    def test_review_reject_and_resubmit(self):
        self.prepare()
        rejected = self.app.campaigns.decide(
            "campaign", 3, "reviewer", False, "Need independent confirmation"
        )
        self.assertEqual(rejected["state"], "collecting")
        self.assertEqual(rejected["revision"], 4)
        self.app.campaigns.submit("campaign", 4)
        approved = self.app.campaigns.decide(
            "campaign", 5, "reviewer2", True, "Evidence checked"
        )
        self.assertEqual(approved["state"], "approved")
        self.assertEqual(approved["revision"], 6)
        decisions = self.app.campaigns.decisions("campaign")
        self.assertEqual(len(decisions), 2)
        self.assertFalse(decisions[0]["approved"])
        self.assertTrue(decisions[1]["approved"])
        with self.assertRaises(StateError):
            self.app.campaigns.submit("campaign", 6)

    def test_data_changes_during_review_block_approval(self):
        self.prepare()
        self.app.pipeline.replay("repair", "i1", "p", START, END)
        with self.assertRaises(Conflict):
            self.app.campaigns.decide("campaign", 3, "reviewer", True, "Approve")
        self.assertEqual(self.app.campaigns.get("campaign")["state"], "review")
        self.assertEqual(self.app.campaigns.decisions("campaign"), [])
        self.app.catalog.revoke_certificate("c1", "invalid calibration")
        self.assertIn(
            "revoked_calibration",
            self.app.campaigns.assess("campaign")["checks"][0]["issues"],
        )

    def test_bundle_contains_reviewed_frozen_evidence(self):
        self.prepare()
        self.app.campaigns.decide("campaign", 3, "reviewer", True, "Checked")
        self.app.windows.materialize(window())
        self.app.windows.seal("w1", 1)
        self.app.snapshots.create("release", ["w1"])
        payload = self.app.bundles.build("campaign", "release")
        self.assertEqual(payload, self.app.bundles.build("campaign", "release"))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "evidence.tar"
            self.app.bundles.export("campaign", "release", path)
            verified = verify_bundle(path)
            self.assertEqual(verified["campaign"], "campaign")
            self.assertEqual(verified["snapshot"], "release")
            self.assertEqual(verified["observations"], 2)
            self.assertEqual(verified["members"], 5)
            with self.assertRaises(Conflict):
                self.app.bundles.export("campaign", "release", path)

    def test_campaign_revision_and_lot_isolation(self):
        self.app.campaigns.plan(self.plan)
        with self.assertRaises(Conflict):
            self.app.campaigns.start("campaign", 2)
        self.app.campaigns.start("campaign", 1)
        self.app.pipeline.ingest(
            "b",
            rows(observation(tags={"lot": "B"}), observation(30, tags={"lot": "B"})),
            received_at=RECEIVED,
        )
        self.app.catalog.watermark("s1", END)
        assessment = self.app.campaigns.assess("campaign")
        self.assertFalse(assessment["ready"])
        self.assertEqual(assessment["checks"][0]["received"], 0)
        self.assertEqual(assessment["lineage"], [])
        with self.assertRaises(Conflict):
            self.app.campaigns.plan({**self.plan, "lot": "B"})


class PersistenceTests(unittest.TestCase):
    def test_close_reopen_and_backup(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            backup = Path(directory) / "backup.sqlite3"
            with application(path) as app:
                published(app)
                manifest = app.inspector.manifest()
                app.database.backup(backup)
                with self.assertRaises(StateError):
                    app.database.backup(backup)
            with Service(path) as app:
                self.assertEqual(app.pipeline.get("r0")["revision"], 1)
                self.assertTrue(app.inspector.compare_manifest(manifest)["same"])
                self.assertEqual(app.inspector.check()["invariants"], "ok")
            with Service(backup) as app:
                self.assertEqual(
                    app.snapshots.get("snapshot")["payload"]["id"], "snapshot"
                )
                self.assertTrue(app.inspector.compare_manifest(manifest)["same"])

    def test_nested_rollback_keeps_outer_transaction(self):
        with application() as app:
            with app.database.transaction() as connection:
                connection.execute("INSERT INTO meta VALUES('outer','yes')")
                with self.assertRaises(RuntimeError):
                    with app.database.transaction() as inner:
                        inner.execute("INSERT INTO meta VALUES('inner','no')")
                        raise RuntimeError("synthetic failure")
                self.assertIsNone(
                    app.database.one("SELECT * FROM meta WHERE key='inner'")
                )
            self.assertEqual(
                app.database.one("SELECT value FROM meta WHERE key='outer'")["value"],
                "yes",
            )
            with app.database.transaction():
                with self.assertRaises(StateError):
                    app.database.close()

    def test_two_writers_agree_on_idempotent_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            with application(path):
                pass
            apps = [Service(path), Service(path)]
            barrier = threading.Barrier(2)
            output = []
            errors = []

            def writer(app):
                try:
                    barrier.wait()
                    output.append(
                        app.pipeline.ingest(
                            "same", rows(observation()), received_at=RECEIVED
                        )
                    )
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=writer, args=(app,)) for app in apps]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
            self.assertFalse(any(thread.is_alive() for thread in threads))
            self.assertEqual(errors, [])
            self.assertEqual(sorted(item["reused"] for item in output), [False, True])
            self.assertEqual(apps[0].reports.overview()["observations"], 1)
            for app in apps:
                app.close()

    def test_tampered_raw_record_fails_invariant_check(self):
        with application() as app:
            app.pipeline.ingest("b", rows(observation()), received_at=RECEIVED)
            with app.database.transaction() as con:
                con.execute(
                    "UPDATE observations SET fingerprint='invalid' WHERE id='r0'"
                )
            with self.assertRaises(IntegrityError) as caught:
                app.inspector.check()
            violations = caught.exception.details["violations"]
            self.assertIn("raw_fingerprint", [item["code"] for item in violations])

    def test_audit_tampering_is_detected(self):
        with application() as app:
            with app.database.transaction() as con:
                con.execute("UPDATE audit SET payload='{}' WHERE sequence=1")
            with self.assertRaises(IntegrityError) as caught:
                app.inspector.check()
            self.assertIn("audit", str(caught.exception))


class ReportAndCliTests(unittest.TestCase):
    def test_pagination_no_duplicates_on_equal_timestamps(self):
        with application() as app:
            values = [observation(i, event_time=START) for i in range(4)]
            app.pipeline.ingest("b", rows(*values), received_at=RECEIVED)
            first = app.reports.query(limit=2)
            second = app.reports.query(limit=2, after=first["next"])
            self.assertEqual([item["id"] for item in first["items"]], ["r0", "r1"])
            self.assertEqual([item["id"] for item in second["items"]], ["r2", "r3"])
            self.assertIsNone(second["next"])
            self.assertEqual(app.reports.query(status="error")["items"], [])
            quality = app.reports.quality("i1", "p", START, END)
            self.assertEqual(quality["total"], 4)
            self.assertEqual(quality["status_counts"], {"accepted": 4})

    def test_quality_errors_and_certificate_impact(self):
        with application() as app:
            published(app)
            app.pipeline.ingest(
                "bad",
                rows(observation(1, value="30"), observation(2, value="200")),
                received_at=RECEIVED,
            )
            quality = app.reports.quality("i1", "p", START, END)
            self.assertEqual(quality["total"], 3)
            self.assertEqual(quality["status_counts"]["error"], 1)
            self.assertEqual(quality["status_counts"]["rejected"], 1)
            self.assertIn("calibration_error", quality["issue_counts"])
            impact = app.reports.certificate_impact("c1")
            self.assertEqual(len(impact["observations"]), 2)
            self.assertEqual(impact["windows"][0]["id"], "w1")
            with self.assertRaises(Missing):
                app.reports.certificate_impact("missing")

    def test_diagnostics_are_deterministic(self):
        self.assertEqual(percentile([1, 3, 5, 7], ".5"), 4)
        summary = robust_summary([1, 1, 1, 1, 100])
        self.assertEqual(summary["median"], "1")
        self.assertEqual(summary["outliers"], [4])
        with application() as app:
            app.pipeline.ingest(
                "b",
                rows(observation(), observation(1), observation(2)),
                received_at=RECEIVED,
            )
            diagnostics = Diagnostics(app.reports)
            profile = diagnostics.profile("i1", "p", START, END)
            self.assertEqual(profile["stable_runs"][0]["count"], 3)
            self.assertEqual(profile["profiles"]["kPa"]["median"], "10")
            trend = diagnostics.trend("i1", "p", START, END)
            self.assertEqual(
                [point["ewma"] for point in trend["points"]], ["10", "10", "10"]
            )
            self.assertTrue(all(not point["alert"] for point in trend["points"]))

    def test_configuration_is_transactional(self):
        with application() as app:
            before = app.inspector.manifest()
            with self.assertRaises(InputError):
                app.configuration.apply(
                    {
                        "id": "bad",
                        "sources": [{"id": "s2"}],
                        "instruments": [{"id": "i2", "label": "bad", "channels": {}}],
                    }
                )
            self.assertTrue(app.inspector.compare_manifest(before)["same"])
            self.assertTrue(app.configuration.readiness("i1", "p", START, END)["ready"])
            self.assertFalse(
                app.configuration.readiness("missing", "p", START, END)["ready"]
            )
            exported = app.configuration.export("copy")
            with Service() as target:
                target.configuration.apply(exported)
                self.assertTrue(
                    target.configuration.readiness("i1", "p", START, END)["ready"]
                )
                self.assertEqual(target.catalog.source("s1")["high_sequence"], -1)

    def test_cli_demo_and_structured_errors(self):
        output = io.StringIO()
        with redirect_stdout(output):
            result = main(["demo"])
        self.assertEqual(result, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["mean"], "11.1")
        self.assertTrue(payload["duplicate_reused"])
        with tempfile.TemporaryDirectory() as directory:
            request = Path(directory) / "request.json"
            request.write_text('{"action":"unknown"}')
            error = io.StringIO()
            with redirect_stderr(error):
                result = main(
                    [
                        "--db",
                        str(Path(directory) / "state.sqlite3"),
                        "run",
                        str(request),
                    ]
                )
            self.assertEqual(result, 2)
            self.assertEqual(
                json.loads(error.getvalue())["error"]["code"], "invalid_input"
            )

    def test_cli_dispatch_reaches_core_services(self):
        with application() as app:
            self.assertEqual(dispatch(app, {"action": "overview"})["instruments"], 1)
            self.assertEqual(dispatch(app, {"action": "check"})["invariants"], "ok")
            changed = dispatch(app, {"action": "watermark", "source": "s1", "at": END})
            self.assertTrue(changed["changed"])
            unchanged = dispatch(
                app, {"action": "watermark", "source": "s1", "at": END}
            )
            self.assertFalse(unchanged["changed"])
            with self.assertRaises(InputError):
                dispatch(app, {"action": "absent"})


class CrosscheckTests(unittest.TestCase):
    def setUp(self):
        self.app = application()
        self.addCleanup(self.app.close)
        self.app.catalog.add_instrument(
            {
                "id": "reference",
                "label": "Synthetic reference",
                "channels": {"p": "kPa"},
            }
        )
        self.app.catalog.add_source("reference-source")
        self.app.catalog.add_certificate(
            certificate(id="reference-cert", instrument="reference")
        )
        self.app.catalog.bind("reference", "p", "q1")

    def crosscheck(self, **options):
        from metrology.processing.crosscheck import PairedChannels

        return PairedChannels(self.app.reports, self.app.catalog.registry()).compare(
            "i1", "p", "reference", "p", START, END, **options
        )

    def test_paired_channel_difference_and_unit(self):
        left = observation(0, value="10")
        right = observation(
            1, id="ref", instrument="reference", source="reference-source", value="10.5"
        )
        self.app.pipeline.ingest("b", rows(left, right), received_at=RECEIVED)
        report = self.crosscheck(
            tolerance_seconds=1, maximum_difference="1000", target_unit="Pa"
        )
        self.assertTrue(report["passed"])
        self.assertEqual(report["unit"], "Pa")
        self.assertEqual(report["bias"], "-500")
        self.assertEqual(report["rmse"], "500")
        self.assertEqual(report["pairs"][0]["left"], "r0")
        self.assertEqual(report["pairs"][0]["right"], "ref")
        self.assertIsNone(report["pairs"][0]["normalized_difference"])
        self.assertEqual(report["unmatched_left"], [])
        self.assertEqual(report["unmatched_right"], [])

    def test_equidistant_references_are_ambiguous(self):
        values = [
            observation(1),
            observation(
                0, id="ref0", instrument="reference", source="reference-source"
            ),
            observation(
                2, id="ref2", instrument="reference", source="reference-source"
            ),
        ]
        self.app.pipeline.ingest("b", rows(*values), received_at=RECEIVED)
        report = self.crosscheck()
        self.assertFalse(report["passed"])
        self.assertEqual(report["pairs"], [])
        self.assertEqual(report["unmatched_left"], ["r1"])
        self.assertEqual(report["ambiguities"][0]["candidates"], ["ref0", "ref2"])
        self.assertEqual(report["unmatched_right"], ["ref0", "ref2"])

    def test_reference_is_consumed_only_once(self):
        values = [
            observation(0),
            observation(1),
            observation(0, id="ref", instrument="reference", source="reference-source"),
        ]
        self.app.pipeline.ingest("b", rows(*values), received_at=RECEIVED)
        report = self.crosscheck()
        self.assertFalse(report["passed"])
        self.assertEqual(len(report["pairs"]), 1)
        self.assertEqual(report["unmatched_left"], ["r1"])
        self.assertEqual(report["unmatched_right"], [])
        self.assertEqual(report["bias"], "0")

    def test_difference_threshold_and_missing_data(self):
        empty = self.crosscheck()
        self.assertFalse(empty["passed"])
        self.assertIsNone(empty["bias"])
        self.assertEqual(empty["pairs"], [])
        values = [
            observation(0, value="10"),
            observation(
                0,
                id="ref",
                instrument="reference",
                source="reference-source",
                value="12",
            ),
        ]
        self.app.pipeline.ingest("b", rows(*values), received_at=RECEIVED)
        failed = self.crosscheck(maximum_difference="1")
        self.assertFalse(failed["passed"])
        self.assertFalse(failed["pairs"][0]["within_limit"])
        passed = self.crosscheck(maximum_difference="2")
        self.assertTrue(passed["passed"])
        with self.assertRaises(InputError):
            self.crosscheck(tolerance_seconds=-1)

    def test_registered_late_source_repairs_high_sequence(self):
        self.app.pipeline.ingest(
            "orphan",
            rows(observation(7, source="late-registration")),
            received_at=RECEIVED,
        )
        self.app.catalog.add_source("late-registration")
        self.assertEqual(
            self.app.catalog.source("late-registration")["high_sequence"], 7
        )
        self.assertEqual(self.app.inspector.check()["invariants"], "ok")
        self.app.pipeline.replay("repair", "i1", "p", START, END)
        self.assertTrue(self.app.pipeline.get("r7")["measurement"]["accepted"])
        self.assertEqual(self.app.pipeline.get("r7")["revision"], 2)
