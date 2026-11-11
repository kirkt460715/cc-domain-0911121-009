from collections import Counter
from ..errors import Conflict, InputError, Missing, StateError
from ..values import (
    require_object,
    identifier,
    integer,
    instant,
    time_text,
    canonical,
    strict_json,
    digest,
)
from ..ledger.audit import append


class Campaigns:
    def __init__(self, database, catalog, reports):
        self.db = database
        self.catalog = catalog
        self.reports = reports
        with self.db.transaction() as con:
            con.execute("""CREATE TABLE IF NOT EXISTS campaigns(id TEXT PRIMARY KEY,specification TEXT NOT NULL,
                fingerprint TEXT NOT NULL,state TEXT NOT NULL,revision INTEGER NOT NULL,assessment TEXT)""")
            con.execute("""CREATE TABLE IF NOT EXISTS campaign_decisions(campaign TEXT NOT NULL,revision INTEGER NOT NULL,
                decision TEXT NOT NULL,PRIMARY KEY(campaign,revision))""")

    def plan(self, data):
        require_object(
            data,
            {"id", "start", "end", "lot", "channels"},
            {"id", "start", "end", "lot", "channels"},
        )
        campaign_id = identifier(data["id"])
        start, end = instant(data["start"]), instant(data["end"])
        if end <= start:
            raise InputError("campaign time interval must increase")
        lot = identifier(data["lot"], "lot")
        raw_channels = data["channels"]
        if not isinstance(raw_channels, list) or not 1 <= len(raw_channels) <= 128:
            raise InputError(
                "campaign requires one to one hundred twenty-eight channels"
            )
        channels = []
        seen = set()
        for item in raw_channels:
            require_object(
                item,
                {
                    "instrument",
                    "channel",
                    "minimum_count",
                    "maximum_gap_seconds",
                    "sources",
                },
                {
                    "instrument",
                    "channel",
                    "minimum_count",
                    "maximum_gap_seconds",
                    "sources",
                },
            )
            instrument = identifier(item["instrument"])
            channel = identifier(item["channel"])
            if (instrument, channel) in seen:
                raise InputError("duplicate campaign channel")
            seen.add((instrument, channel))
            self.catalog.channel(instrument, channel)
            self.catalog.bound_policy(instrument, channel)
            minimum = integer(item["minimum_count"], "minimum_count", 1, 1000000)
            gap = integer(
                item["maximum_gap_seconds"], "maximum_gap_seconds", 1, 31536000
            )
            sources = item["sources"]
            if (
                not isinstance(sources, list)
                or not sources
                or len(set(sources)) != len(sources)
            ):
                raise InputError("campaign sources must be nonempty and unique")
            for source in sources:
                self.catalog.source(identifier(source))
            channels.append(
                {
                    "instrument": instrument,
                    "channel": channel,
                    "minimum_count": minimum,
                    "maximum_gap_seconds": gap,
                    "sources": sorted(sources),
                }
            )
        channels.sort(key=lambda item: (item["instrument"], item["channel"]))
        spec = {
            "id": campaign_id,
            "start": time_text(start),
            "end": time_text(end),
            "lot": lot,
            "channels": channels,
        }
        fingerprint = digest(spec)
        with self.db.transaction() as con:
            row = con.execute(
                "SELECT fingerprint FROM campaigns WHERE id=?", (campaign_id,)
            ).fetchone()
            if row:
                if row["fingerprint"] != fingerprint:
                    raise Conflict("campaign plan is immutable")
                return self.get(campaign_id)
            con.execute(
                "INSERT INTO campaigns VALUES(?,?,?,'planned',1,NULL)",
                (campaign_id, canonical(spec), fingerprint),
            )
            append(con, "campaign_planned", campaign_id, spec)
        return self.get(campaign_id)

    def get(self, campaign_id):
        row = self.db.one("SELECT * FROM campaigns WHERE id=?", (campaign_id,))
        if row is None:
            raise Missing("campaign not found")
        return {
            "id": campaign_id,
            "state": row["state"],
            "revision": row["revision"],
            "specification": strict_json(row["specification"]),
            "assessment": strict_json(row["assessment"]) if row["assessment"] else None,
        }

    def start(self, campaign_id, expected_revision):
        with self.db.transaction() as con:
            campaign = self.get(campaign_id)
            if campaign["revision"] != expected_revision:
                raise Conflict("campaign revision changed")
            if campaign["state"] != "planned":
                raise StateError("only a planned campaign can start")
            spec = campaign["specification"]
            for item in spec["channels"]:
                gaps = self.catalog.certificates().gaps(
                    item["instrument"],
                    item["channel"],
                    instant(spec["start"]),
                    instant(spec["end"]),
                )
                if gaps:
                    raise StateError(
                        "campaign requires full calibration coverage",
                        details={"channel": item["channel"], "gaps": gaps},
                    )
            con.execute(
                "UPDATE campaigns SET state='collecting',revision=revision+1 WHERE id=?",
                (campaign_id,),
            )
            append(
                con,
                "campaign_started",
                campaign_id,
                {"revision": expected_revision + 1},
            )
        return self.get(campaign_id)

    def assess(self, campaign_id):
        campaign = self.get(campaign_id)
        spec = campaign["specification"]
        checks = []
        lineage = []
        for channel in spec["channels"]:
            cursor = None
            items = []
            while True:
                page = self.reports.query(
                    instrument=channel["instrument"],
                    channel=channel["channel"],
                    start=spec["start"],
                    end=spec["end"],
                    after=cursor,
                    limit=10000,
                )
                for item in page["items"]:
                    if item["source"] not in channel["sources"]:
                        continue
                    raw = self.db.one(
                        "SELECT raw FROM observations WHERE id=?", (item["id"],)
                    )
                    if strict_json(raw["raw"])["tags"].get("lot") != spec["lot"]:
                        continue
                    items.append(item)
                    lineage.append({"id": item["id"], "revision": item["revision"]})
                cursor = page["next"]
                if cursor is None:
                    break
            accepted = [item for item in items if item["status"] == "accepted"]
            boundaries = (
                [instant(spec["start"])]
                + [instant(item["event_time"]) for item in accepted]
                + [instant(spec["end"])]
            )
            gaps = [
                (right - left).total_seconds()
                for left, right in zip(boundaries, boundaries[1:])
            ]
            largest_gap = max(gaps)
            closed_sources = []
            for source in channel["sources"]:
                state = self.catalog.source(source)
                cutoff = instant(state["watermark"]) if state["watermark"] else None
                from datetime import timedelta

                closed_sources.append(
                    cutoff is not None
                    and cutoff - timedelta(seconds=state["allowed_lateness"])
                    >= instant(spec["end"])
                )
            issues = []
            if len(accepted) < channel["minimum_count"]:
                issues.append("insufficient_count")
            if largest_gap > channel["maximum_gap_seconds"]:
                issues.append("sampling_gap")
            if len(accepted) != len(items):
                issues.append("quality_failure")
            if not all(closed_sources):
                issues.append("sources_not_final")
            active = {cert.id for cert in self.catalog.certificates().active()}
            if any(
                item["measurement"]["certificate"] not in active for item in accepted
            ):
                issues.append("revoked_calibration")
            checks.append(
                {
                    "instrument": channel["instrument"],
                    "channel": channel["channel"],
                    "received": len(items),
                    "accepted": len(accepted),
                    "largest_gap_seconds": largest_gap,
                    "status_counts": dict(Counter(item["status"] for item in items)),
                    "issues": issues,
                }
            )
        result = {
            "id": campaign_id,
            "ready": all(not check["issues"] for check in checks),
            "checks": checks,
            "lineage": sorted(lineage, key=lambda item: item["id"]),
        }
        result["fingerprint"] = digest(result)
        return result

    def submit(self, campaign_id, expected_revision):
        with self.db.transaction() as con:
            campaign = self.get(campaign_id)
            if campaign["revision"] != expected_revision:
                raise Conflict("campaign revision changed")
            if campaign["state"] != "collecting":
                raise StateError("only a collecting campaign can be submitted")
            assessment = self.assess(campaign_id)
            if not assessment["ready"]:
                raise StateError(
                    "campaign does not meet its sampling plan", details=assessment
                )
            con.execute(
                "UPDATE campaigns SET state='review',revision=revision+1,assessment=? WHERE id=?",
                (canonical(assessment), campaign_id),
            )
            append(
                con,
                "campaign_submitted",
                campaign_id,
                {
                    "revision": expected_revision + 1,
                    "assessment": assessment["fingerprint"],
                },
            )
        return self.get(campaign_id)

    def decide(self, campaign_id, expected_revision, reviewer, approved, reason):
        from ..values import label

        identifier(reviewer, "reviewer")
        reason = label(reason, "reason")
        if not isinstance(approved, bool):
            raise InputError("decision must be boolean")
        with self.db.transaction() as con:
            campaign = self.get(campaign_id)
            if campaign["revision"] != expected_revision:
                raise Conflict("campaign revision changed")
            if campaign["state"] != "review":
                raise StateError("campaign is not awaiting review")
            if approved:
                current = self.assess(campaign_id)
                if (
                    not current["ready"]
                    or current["fingerprint"] != campaign["assessment"]["fingerprint"]
                ):
                    raise Conflict(
                        "campaign data or qualification changed during review"
                    )
            decision = {
                "reviewer": reviewer,
                "approved": approved,
                "reason": reason,
                "assessment": campaign["assessment"]["fingerprint"],
            }
            con.execute(
                "INSERT INTO campaign_decisions VALUES(?,?,?)",
                (campaign_id, expected_revision + 1, canonical(decision)),
            )
            con.execute(
                "UPDATE campaigns SET state=?,revision=revision+1 WHERE id=?",
                ("approved" if approved else "collecting", campaign_id),
            )
            append(con, "campaign_decided", campaign_id, decision)
        return self.get(campaign_id)

    def decisions(self, campaign_id):
        self.get(campaign_id)
        rows = self.db.rows(
            "SELECT revision,decision FROM campaign_decisions WHERE campaign=? ORDER BY revision",
            (campaign_id,),
        )
        return [
            {"revision": row["revision"], **strict_json(row["decision"])}
            for row in rows
        ]
