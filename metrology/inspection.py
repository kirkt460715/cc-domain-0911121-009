from .errors import IntegrityError, InputError
from .values import digest, strict_json
from .observations import Observation
from .ledger.audit import verify


class Inspector:
    def __init__(self, database):
        self.db = database

    def check(self):
        database = self.db.integrity()
        chain = verify(self.db)
        errors = []
        observations = self.db.rows("SELECT * FROM observations ORDER BY id")
        for row in observations:
            observation = Observation.parse(strict_json(row["raw"]))
            if observation.fingerprint != row["fingerprint"]:
                errors.append({"id": row["id"], "code": "raw_fingerprint"})
            if (
                observation.id != row["id"]
                or observation.source != row["source"]
                or observation.sequence != row["sequence"]
            ):
                errors.append({"id": row["id"], "code": "raw_index_mismatch"})
            revisions = self.db.rows(
                "SELECT * FROM results WHERE observation=? ORDER BY revision",
                (row["id"],),
            )
            if [revision["revision"] for revision in revisions] != list(
                range(1, len(revisions) + 1)
            ):
                errors.append({"id": row["id"], "code": "revision_gap"})
            if sum(revision["active"] for revision in revisions) != 1:
                errors.append({"id": row["id"], "code": "active_result_count"})
            if revisions and not revisions[-1]["active"]:
                errors.append({"id": row["id"], "code": "active_result_not_latest"})
            for revision in revisions:
                if bool(revision["payload"]) == bool(revision["error"]):
                    errors.append(
                        {"id": row["id"], "code": "result_payload_exclusivity"}
                    )
                if revision["payload"]:
                    result = strict_json(revision["payload"])
                    if result["observation"] != observation.as_dict():
                        errors.append({"id": row["id"], "code": "result_raw_mismatch"})
                    expected = not any(
                        issue["severity"] == "reject" for issue in result["issues"]
                    )
                    if result["accepted"] != expected:
                        errors.append(
                            {"id": row["id"], "code": "quality_flag_mismatch"}
                        )
            if (
                self.db.one("SELECT id FROM batches WHERE id=?", (row["batch"],))
                is None
            ):
                errors.append({"id": row["id"], "code": "missing_batch"})
        for row in self.db.rows("SELECT * FROM batches ORDER BY id"):
            if digest(strict_json(row["payload"])) != row["fingerprint"]:
                errors.append({"id": row["id"], "code": "batch_fingerprint"})
            count = self.db.one(
                "SELECT COUNT(*) AS n FROM observations WHERE batch=?", (row["id"],)
            )["n"]
            result = strict_json(row["result"])
            if result["inserted"] != count:
                errors.append({"id": row["id"], "code": "batch_count"})
        for row in self.db.rows("SELECT * FROM snapshots ORDER BY id"):
            if digest(strict_json(row["payload"])) != row["fingerprint"]:
                errors.append({"id": row["id"], "code": "snapshot_fingerprint"})
        for row in self.db.rows("SELECT * FROM sources ORDER BY id"):
            maximum = self.db.one(
                "SELECT MAX(sequence) AS n FROM observations WHERE source=?",
                (row["id"],),
            )["n"]
            maximum = maximum if maximum is not None else -1
            if row["high_sequence"] != maximum:
                errors.append({"id": row["id"], "code": "sequence_high_water"})
        if errors:
            raise IntegrityError(
                "application invariants failed", details={"violations": errors}
            )
        return {
            "database": database,
            "audit": chain,
            "observations": len(observations),
            "invariants": "ok",
        }

    def stale_windows(self):
        result = []
        for row in self.db.rows("SELECT id,payload,sealed FROM windows ORDER BY id"):
            payload = strict_json(row["payload"])
            stale = []
            for window in payload["windows"]:
                for reference in window["lineage"]:
                    current = self.db.one(
                        "SELECT revision FROM results WHERE observation=? AND active=1",
                        (reference["id"],),
                    )
                    if current is None or current["revision"] != reference["revision"]:
                        stale.append(
                            {
                                "id": reference["id"],
                                "frozen_revision": reference["revision"],
                                "current_revision": current["revision"]
                                if current
                                else None,
                            }
                        )
            if stale:
                result.append(
                    {"window": row["id"], "sealed": bool(row["sealed"]), "stale": stale}
                )
        return result

    def manifest(self):
        objects = []
        tables = {
            "units": ("symbol",),
            "instruments": ("id",),
            "certificates": ("id",),
            "policies": ("id",),
            "bindings": ("instrument", "channel"),
            "sources": ("id",),
            "observations": ("id",),
            "results": ("observation", "revision"),
            "batches": ("id",),
            "quarantines": ("batch", "line"),
            "replays": ("id",),
            "windows": ("id",),
            "snapshots": ("id",),
        }
        for table, keys in tables.items():
            rows = self.db.rows(
                "SELECT * FROM " + table + " ORDER BY " + ",".join(keys)
            )
            objects.append(
                {"table": table, "rows": len(rows), "fingerprint": digest(rows)}
            )
        return {"schema": 1, "tables": objects, "fingerprint": digest(objects)}

    def compare_manifest(self, other):
        if (
            not isinstance(other, dict)
            or other.get("schema") != 1
            or not isinstance(other.get("tables"), list)
        ):
            raise InputError("invalid database manifest")
        if digest(other["tables"]) != other.get("fingerprint"):
            raise IntegrityError("manifest fingerprint mismatch")
        current = self.manifest()
        before = {item["table"]: item for item in other["tables"]}
        after = {item["table"]: item for item in current["tables"]}
        changed = []
        for key in sorted(before.keys() | after.keys()):
            if before.get(key) != after.get(key):
                changed.append(
                    {"table": key, "before": before.get(key), "after": after.get(key)}
                )
        return {"same": not changed, "changed": changed}
