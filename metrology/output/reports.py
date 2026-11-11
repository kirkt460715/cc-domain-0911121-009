from collections import Counter
from ..errors import InputError, Missing
from ..values import instant, time_text, strict_json, identifier, integer
from ..observations import Measurement
from ..processing.windows import statistics


class Reports:
    def __init__(self, database, catalog):
        self.db = database
        self.catalog = catalog

    def query(
        self,
        *,
        instrument=None,
        channel=None,
        source=None,
        start=None,
        end=None,
        status=None,
        after=None,
        limit=100,
    ):
        integer(limit, "limit", 1, 10000)
        if status not in (None, "accepted", "rejected", "error"):
            raise InputError("unsupported result status")
        conditions = ["r.active=1"]
        parameters = []
        for key, value in (
            ("instrument", instrument),
            ("channel", channel),
            ("source", source),
        ):
            if value is not None:
                identifier(value, key)
                conditions.append("o." + key + "=?")
                parameters.append(value)
        if start is not None:
            conditions.append("o.event_time>=?")
            parameters.append(time_text(instant(start)))
        if end is not None:
            conditions.append("o.event_time<?")
            parameters.append(time_text(instant(end)))
        if start is not None and end is not None and instant(start) >= instant(end):
            raise InputError("report interval must increase")
        if after is not None:
            if not isinstance(after, dict) or set(after) != {"time", "id"}:
                raise InputError("invalid pagination cursor")
            at = time_text(instant(after["time"]))
            ident = identifier(after["id"])
            conditions.append("(o.event_time>? OR (o.event_time=? AND o.id>?))")
            parameters.extend([at, at, ident])
        sql = """SELECT o.*,r.payload,r.error,r.revision FROM observations o JOIN results r ON r.observation=o.id WHERE """
        sql += " AND ".join(conditions) + " ORDER BY o.event_time,o.id"
        rows = self.db.rows(sql, parameters)
        selected = []
        for row in rows:
            payload = strict_json(row["payload"]) if row["payload"] else None
            state = (
                "error"
                if row["error"]
                else ("accepted" if payload["accepted"] else "rejected")
            )
            if status is not None and state != status:
                continue
            selected.append(
                {
                    "id": row["id"],
                    "source": row["source"],
                    "sequence": row["sequence"],
                    "event_time": row["event_time"],
                    "instrument": row["instrument"],
                    "channel": row["channel"],
                    "revision": row["revision"],
                    "status": state,
                    "measurement": payload,
                    "error": strict_json(row["error"]) if row["error"] else None,
                }
            )
            if len(selected) > limit:
                break
        more = len(selected) > limit
        selected = selected[:limit]
        cursor = (
            {"time": selected[-1]["event_time"], "id": selected[-1]["id"]}
            if more
            else None
        )
        return {"items": selected, "next": cursor}

    def quality(self, instrument, channel, start, end):
        cursor = None
        counts = Counter()
        issue_counts = Counter()
        accepted = []
        total = 0
        while True:
            page = self.query(
                instrument=instrument,
                channel=channel,
                start=start,
                end=end,
                after=cursor,
                limit=10000,
            )
            for item in page["items"]:
                total += 1
                counts[item["status"]] += 1
                if item["measurement"]:
                    measured = Measurement.restore(item["measurement"])
                    for issue in measured.issues:
                        issue_counts[issue.code] += 1
                    if measured.accepted:
                        accepted.append(measured)
                else:
                    issue_counts[item["error"]["code"]] += 1
            cursor = page["next"]
            if cursor is None:
                break
        unit_groups = {}
        for item in accepted:
            unit_groups.setdefault(item.unit, []).append(item)
        return {
            "instrument": instrument,
            "channel": channel,
            "start": time_text(instant(start)),
            "end": time_text(instant(end)),
            "total": total,
            "status_counts": dict(sorted(counts.items())),
            "issue_counts": dict(sorted(issue_counts.items())),
            "statistics_by_unit": {
                unit: statistics(items) for unit, items in sorted(unit_groups.items())
            },
        }

    def sequence_gaps(self, source):
        state = self.catalog.source(source)
        rows = self.db.rows(
            "SELECT sequence FROM observations WHERE source=? ORDER BY sequence",
            (source,),
        )
        gaps = []
        expected = 0
        for row in rows:
            sequence = row["sequence"]
            if sequence > expected:
                gaps.append(
                    {
                        "first": expected,
                        "last": sequence - 1,
                        "count": sequence - expected,
                    }
                )
            expected = sequence + 1
        return {
            "source": source,
            "high_sequence": state["high_sequence"],
            "missing": sum(gap["count"] for gap in gaps),
            "gaps": gaps,
        }

    def batch(self, batch_id):
        row = self.db.one("SELECT * FROM batches WHERE id=?", (batch_id,))
        if row is None:
            raise Missing("batch not found")
        quarantine = self.db.rows(
            "SELECT line,payload FROM quarantines WHERE batch=? ORDER BY line",
            (batch_id,),
        )
        return {
            "id": batch_id,
            "fingerprint": row["fingerprint"],
            "created": row["created"],
            "result": strict_json(row["result"]),
            "quarantines": [
                {"line": q["line"], "error": strict_json(q["payload"])}
                for q in quarantine
            ],
        }

    def certificate_impact(self, certificate_id):
        if (
            self.db.one("SELECT id FROM certificates WHERE id=?", (certificate_id,))
            is None
        ):
            raise Missing("certificate not found")
        rows = self.db.rows(
            "SELECT observation,revision,payload,active FROM results WHERE payload IS NOT NULL ORDER BY observation,revision"
        )
        references = []
        for row in rows:
            payload = strict_json(row["payload"])
            if payload["certificate"] == certificate_id:
                references.append(
                    {
                        "observation": row["observation"],
                        "revision": row["revision"],
                        "active": bool(row["active"]),
                    }
                )
        windows = []
        active_ids = {row["observation"] for row in references if row["active"]}
        for row in self.db.rows("SELECT id,payload FROM windows ORDER BY id"):
            payload = strict_json(row["payload"])
            if any(
                ref["id"] in active_ids
                for window in payload["windows"]
                for ref in window["lineage"]
            ):
                windows.append(
                    {
                        "id": row["id"],
                        "sealed": payload["sealed"],
                        "generation": payload["generation"],
                    }
                )
        return {
            "certificate": certificate_id,
            "observations": references,
            "windows": windows,
        }

    def overview(self):
        tables = (
            "instruments",
            "certificates",
            "policies",
            "sources",
            "observations",
            "batches",
            "replays",
            "windows",
            "snapshots",
        )
        counts = {
            table: self.db.one("SELECT COUNT(*) AS n FROM " + table)["n"]
            for table in tables
        }
        counts["quarantines"] = self.db.one("SELECT COUNT(*) AS n FROM quarantines")[
            "n"
        ]
        return counts
