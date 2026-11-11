import datetime as dt
from ..errors import Conflict, InputError, MetrologyError, Missing, StateError
from ..values import canonical, digest, identifier, instant, time_text, strict_json
from ..observations import Measurement, Observation, Issue
from ..ingress.readers import InputRow
from ..ledger.audit import append


class Pipeline:
    def __init__(self, database, catalog):
        self.db = database
        self.catalog = catalog

    def previous(self, observation):
        rows = self.db.rows(
            """SELECT r.payload FROM observations o JOIN results r ON r.observation=o.id
            WHERE r.active=1 AND r.payload IS NOT NULL AND o.instrument=? AND o.channel=?
            AND (o.event_time<? OR (o.event_time=? AND o.id<?)) ORDER BY o.event_time DESC,o.id DESC LIMIT 1""",
            (
                observation.instrument,
                observation.channel,
                time_text(observation.event_time),
                time_text(observation.event_time),
                observation.id,
            ),
        )
        return Measurement.restore(strict_json(rows[0]["payload"])) if rows else None

    def measure(self, observation, *, check_lateness=True):
        registry = self.catalog.registry()
        channel_unit = self.catalog.channel(observation.instrument, observation.channel)
        registry.validate_pair(observation.unit, channel_unit)
        source = self.catalog.source(observation.source)
        if not source["active"]:
            raise StateError("source is inactive")
        certificate = self.catalog.certificates().select(
            observation.instrument, observation.channel, observation.event_time
        )
        value, uncertainty, unit = certificate.correct(
            observation.value, observation.unit, observation.uncertainty, registry
        )
        policy = self.catalog.bound_policy(observation.instrument, observation.channel)
        measurement = policy.evaluate(
            observation,
            value,
            uncertainty,
            unit,
            certificate.id,
            registry,
            self.previous(observation),
        )
        if check_lateness and source["watermark"] is not None:
            cutoff = instant(source["watermark"]) - dt.timedelta(
                seconds=source["allowed_lateness"]
            )
            if observation.event_time < cutoff:
                issue = Issue(
                    "late",
                    "reject",
                    "observation precedes lateness boundary",
                    {"cutoff": time_text(cutoff)},
                )
                measurement = Measurement(
                    observation,
                    measurement.value,
                    measurement.uncertainty,
                    measurement.unit,
                    measurement.certificate,
                    measurement.policy,
                    measurement.issues + (issue,),
                )
        return measurement

    def ingest(self, batch_id, rows, *, received_at, atomic=False):
        identifier(batch_id, "batch")
        timestamp = time_text(instant(received_at, "received_at"))
        if not isinstance(atomic, bool):
            raise InputError("atomic must be boolean")
        rows = list(rows)
        if not rows or len(rows) > 100000:
            raise InputError("batch must contain one to one hundred thousand rows")
        if any(not isinstance(row, InputRow) for row in rows):
            raise InputError("ingestion requires parsed input rows")
        if len({row.line for row in rows}) != len(rows):
            raise InputError("batch row positions must be unique")
        content = {
            "rows": [row.as_dict() for row in rows],
            "atomic": atomic,
            "received_at": timestamp,
        }
        fingerprint = digest(content)
        with self.db.transaction() as con:
            old = con.execute(
                "SELECT fingerprint,result FROM batches WHERE id=?", (batch_id,)
            ).fetchone()
            if old:
                if old["fingerprint"] != fingerprint:
                    raise Conflict("batch id reused with different content")
                return {**strict_json(old["result"]), "reused": True}
            result = {
                "batch": batch_id,
                "received": len(rows),
                "inserted": 0,
                "duplicates": 0,
                "accepted": 0,
                "rejected": 0,
                "quarantined": 0,
                "reused": False,
            }
            ordered = sorted(
                rows,
                key=lambda row: (
                    (0, time_text(row.observation.event_time), row.observation.id)
                    if row.valid
                    else (1, str(row.line), "")
                ),
            )
            for row in ordered:
                if not row.valid:
                    if atomic:
                        raise InputError(
                            "atomic batch contains malformed row",
                            details={"line": row.line},
                        )
                    self.quarantine(con, batch_id, row.line, row.error.as_dict())
                    result["quarantined"] += 1
                    continue
                observation = row.observation
                previous = con.execute(
                    "SELECT fingerprint FROM observations WHERE id=?", (observation.id,)
                ).fetchone()
                if previous:
                    if previous["fingerprint"] != observation.fingerprint:
                        raise Conflict(
                            "observation id content conflict", field=observation.id
                        )
                    result["duplicates"] += 1
                    continue
                collision = con.execute(
                    "SELECT id FROM observations WHERE source=? AND sequence=?",
                    (observation.source, observation.sequence),
                ).fetchone()
                if collision:
                    raise Conflict(
                        "source sequence occupied by a different observation",
                        details={"existing": collision["id"]},
                    )
                try:
                    measurement = self.measure(observation)
                    error = None
                except MetrologyError as exc:
                    measurement = None
                    error = exc.as_dict()
                if atomic and (error or not measurement.accepted):
                    raise InputError(
                        "atomic batch fails quality validation",
                        details={"id": observation.id, "error": error},
                    )
                con.execute(
                    "INSERT INTO observations VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        observation.id,
                        observation.source,
                        observation.sequence,
                        observation.instrument,
                        observation.channel,
                        time_text(observation.event_time),
                        canonical(observation.as_dict()),
                        observation.fingerprint,
                        batch_id,
                    ),
                )
                con.execute(
                    "INSERT INTO results VALUES(?,1,?,?,1)",
                    (
                        observation.id,
                        canonical(measurement.as_dict()) if measurement else None,
                        canonical(error) if error else None,
                    ),
                )
                result["inserted"] += 1
                if error:
                    result["quarantined"] += 1
                elif measurement.accepted:
                    result["accepted"] += 1
                else:
                    result["rejected"] += 1
                con.execute(
                    "UPDATE sources SET high_sequence=MAX(high_sequence,?) WHERE id=?",
                    (observation.sequence, observation.source),
                )
            con.execute(
                "INSERT INTO batches VALUES(?,?,?,?,?)",
                (
                    batch_id,
                    fingerprint,
                    canonical(content),
                    canonical(result),
                    timestamp,
                ),
            )
            append(con, "batch_ingested", batch_id, result)
        return result

    @staticmethod
    def quarantine(connection, batch_id, line, error):
        connection.execute(
            "INSERT INTO quarantines VALUES(?,?,?)", (batch_id, line, canonical(error))
        )

    def get(self, observation_id, revision=None):
        identifier(observation_id)
        raw = self.db.one("SELECT * FROM observations WHERE id=?", (observation_id,))
        if raw is None:
            raise Missing("observation not found")
        if revision is None:
            result = self.db.one(
                "SELECT * FROM results WHERE observation=? AND active=1",
                (observation_id,),
            )
        else:
            result = self.db.one(
                "SELECT * FROM results WHERE observation=? AND revision=?",
                (observation_id, revision),
            )
        if result is None:
            raise Missing("observation revision not found")
        return {
            "raw": strict_json(raw["raw"]),
            "batch": raw["batch"],
            "revision": result["revision"],
            "measurement": strict_json(result["payload"])
            if result["payload"]
            else None,
            "error": strict_json(result["error"]) if result["error"] else None,
        }

    def history(self, observation_id):
        self.get(observation_id)
        rows = self.db.rows(
            "SELECT * FROM results WHERE observation=? ORDER BY revision",
            (observation_id,),
        )
        return [
            {
                "revision": row["revision"],
                "active": bool(row["active"]),
                "measurement": strict_json(row["payload"]) if row["payload"] else None,
                "error": strict_json(row["error"]) if row["error"] else None,
            }
            for row in rows
        ]

    def replay(
        self, replay_id, instrument, channel, start, end, *, expected_revisions=None
    ):
        identifier(replay_id)
        start_time, end_time = instant(start), instant(end)
        if start_time >= end_time:
            raise InputError("replay interval must increase")
        expected_revisions = expected_revisions or {}
        request = {
            "instrument": instrument,
            "channel": channel,
            "start": time_text(start_time),
            "end": time_text(end_time),
            "expected_revisions": expected_revisions,
        }
        fingerprint = digest(request)
        with self.db.transaction() as con:
            old = con.execute(
                "SELECT * FROM replays WHERE id=?", (replay_id,)
            ).fetchone()
            if old:
                if old["fingerprint"] != fingerprint:
                    raise Conflict("replay id reused with a different interval")
                return {**strict_json(old["result"]), "reused": True}
            self.catalog.channel(instrument, channel)
            rows = self.db.rows(
                """SELECT o.*,r.revision FROM observations o JOIN results r ON r.observation=o.id
                WHERE r.active=1 AND o.instrument=? AND o.channel=? AND o.event_time>=? AND o.event_time<?
                ORDER BY o.event_time,o.id""",
                (instrument, channel, time_text(start_time), time_text(end_time)),
            )
            actual_ids = {row["id"] for row in rows}
            if set(expected_revisions) - actual_ids:
                raise Conflict(
                    "expected revision references an observation outside replay"
                )
            result = {
                "replay": replay_id,
                "changed": 0,
                "accepted": 0,
                "rejected": 0,
                "errors": 0,
                "reused": False,
            }
            for row in rows:
                if (
                    row["id"] in expected_revisions
                    and expected_revisions[row["id"]] != row["revision"]
                ):
                    raise Conflict("stale observation revision", field=row["id"])
                observation = Observation.parse(strict_json(row["raw"]))
                try:
                    measured = self.measure(observation, check_lateness=False)
                    payload, error = canonical(measured.as_dict()), None
                    result["accepted" if measured.accepted else "rejected"] += 1
                except MetrologyError as exc:
                    payload, error = None, canonical(exc.as_dict())
                    result["errors"] += 1
                con.execute(
                    "UPDATE results SET active=0 WHERE observation=? AND active=1",
                    (observation.id,),
                )
                con.execute(
                    "INSERT INTO results VALUES(?,?,?,?,1)",
                    (observation.id, row["revision"] + 1, payload, error),
                )
                result["changed"] += 1
            con.execute(
                "INSERT INTO replays VALUES(?,?,?)",
                (replay_id, fingerprint, canonical(result)),
            )
            append(con, "observations_replayed", replay_id, {**request, **result})
        return result
