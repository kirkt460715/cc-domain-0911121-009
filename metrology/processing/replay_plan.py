from ..errors import InputError, Conflict, Missing, MetrologyError
from ..values import (
    identifier,
    instant,
    time_text,
    canonical,
    strict_json,
    digest,
)
from ..observations import Observation
from ..ledger.audit import append


class ReplayPlanner:
    def __init__(self, database, pipeline, catalog):
        self.db = database
        self.pipeline = pipeline
        self.catalog = catalog
        with self.db.transaction() as con:
            con.execute("""CREATE TABLE IF NOT EXISTS replay_plans(id TEXT PRIMARY KEY,request TEXT NOT NULL,
                preview TEXT NOT NULL,state TEXT NOT NULL,result TEXT)""")

    def plan(self, plan_id, instrument, channel, start, end):
        identifier(plan_id)
        start, end = instant(start), instant(end)
        if end <= start:
            raise InputError("replay plan interval must increase")
        request = {
            "instrument": instrument,
            "channel": channel,
            "start": time_text(start),
            "end": time_text(end),
        }
        with self.db.transaction() as con:
            previous = con.execute(
                "SELECT * FROM replay_plans WHERE id=?", (plan_id,)
            ).fetchone()
            if previous:
                if previous["request"] != canonical(request):
                    raise Conflict("replay plan id has different selection")
                return self.get(plan_id)
            self.catalog.channel(instrument, channel)
            rows = self.db.rows(
                """SELECT o.raw,o.id,r.revision,r.payload,r.error FROM observations o
                JOIN results r ON r.observation=o.id WHERE r.active=1 AND o.instrument=? AND o.channel=?
                AND o.event_time>=? AND o.event_time<? ORDER BY o.event_time,o.id""",
                (instrument, channel, time_text(start), time_text(end)),
            )
            revisions = {row["id"]: row["revision"] for row in rows}
            changes = []
            policy = self.catalog.bound_policy(instrument, channel)
            for row in rows:
                observation = Observation.parse(strict_json(row["raw"]))
                try:
                    measured = self.pipeline.measure(observation, check_lateness=False)
                    after = measured.as_dict()
                    error = None
                except MetrologyError as exc:
                    after = None
                    error = exc.as_dict()
                before = strict_json(row["payload"]) if row["payload"] else None
                before_error = strict_json(row["error"]) if row["error"] else None
                changes.append(
                    {
                        "id": row["id"],
                        "revision": row["revision"],
                        "before": before,
                        "after": after,
                        "before_error": before_error,
                        "after_error": error,
                        "changed": before != after or before_error != error,
                    }
                )
            certificates = self.db.rows(
                "SELECT id,fingerprint,revoked FROM certificates WHERE instrument=? AND channel=? ORDER BY id",
                (instrument, channel),
            )
            preview = {
                "id": plan_id,
                "request": request,
                "expected_revisions": revisions,
                "configuration_fingerprint": digest(
                    {"policy": policy.as_dict(), "certificates": certificates}
                ),
                "changes": changes,
                "stateful_policy": policy.maximum_step is not None
                or policy.maximum_gap_seconds > 0,
                "scope": "Per-observation preview; chronological replay recomputes predecessor-dependent rules.",
            }
            con.execute(
                "INSERT INTO replay_plans VALUES(?,?,?,'planned',NULL)",
                (plan_id, canonical(request), canonical(preview)),
            )
            append(
                con,
                "replay_planned",
                plan_id,
                {
                    "observations": len(rows),
                    "configuration": preview["configuration_fingerprint"],
                },
            )
        return self.get(plan_id)

    def get(self, plan_id):
        row = self.db.one("SELECT * FROM replay_plans WHERE id=?", (plan_id,))
        if row is None:
            raise Missing("replay plan not found")
        return {
            "id": plan_id,
            "state": row["state"],
            "preview": strict_json(row["preview"]),
            "result": strict_json(row["result"]) if row["result"] else None,
        }

    def execute(self, plan_id):
        with self.db.transaction() as con:
            plan = self.get(plan_id)
            if plan["state"] == "executed":
                return plan
            preview = plan["preview"]
            request = preview["request"]
            policy = self.catalog.bound_policy(
                request["instrument"], request["channel"]
            )
            certificates = self.db.rows(
                "SELECT id,fingerprint,revoked FROM certificates WHERE instrument=? AND channel=? ORDER BY id",
                (request["instrument"], request["channel"]),
            )
            fingerprint = digest(
                {"policy": policy.as_dict(), "certificates": certificates}
            )
            if fingerprint != preview["configuration_fingerprint"]:
                raise Conflict("replay configuration changed after planning")
            current = self.db.rows(
                """SELECT o.id FROM observations o WHERE o.instrument=? AND o.channel=?
                AND o.event_time>=? AND o.event_time<? ORDER BY o.id""",
                (
                    request["instrument"],
                    request["channel"],
                    request["start"],
                    request["end"],
                ),
            )
            if {row["id"] for row in current} != set(preview["expected_revisions"]):
                raise Conflict("replay observation selection changed")
            result = self.pipeline.replay(
                plan_id, **request, expected_revisions=preview["expected_revisions"]
            )
            con.execute(
                "UPDATE replay_plans SET state='executed',result=? WHERE id=?",
                (canonical(result), plan_id),
            )
            append(con, "replay_plan_executed", plan_id, result)
        return self.get(plan_id)
