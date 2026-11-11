import csv
import io
import os
import tempfile
from pathlib import Path
from ..errors import Conflict, InputError, IntegrityError, Missing, StateError
from ..values import identifier, canonical, strict_json, digest
from ..ledger.audit import append


class Snapshots:
    def __init__(self, database, windows):
        self.db = database
        self.windows = windows

    def create(self, snapshot_id, window_ids):
        identifier(snapshot_id)
        if not isinstance(window_ids, list) or not window_ids or len(window_ids) > 1000:
            raise InputError("snapshot requires a bounded list of windows")
        if len(set(window_ids)) != len(window_ids):
            raise InputError("duplicate window reference")
        window_ids = sorted(identifier(value) for value in window_ids)
        with self.db.transaction() as con:
            windows = [self.windows.get(value) for value in window_ids]
            if any(not window["sealed"] for window in windows):
                raise StateError("snapshot can include only sealed materializations")
            references = {}
            for group in windows:
                for window in group["windows"]:
                    for reference in window["lineage"]:
                        key = (reference["id"], reference["revision"])
                        if key in references:
                            continue
                        row = con.execute(
                            "SELECT payload,error FROM results WHERE observation=? AND revision=?",
                            key,
                        ).fetchone()
                        if row is None:
                            raise IntegrityError(
                                "window references a missing observation revision"
                            )
                        references[key] = {
                            "id": key[0],
                            "revision": key[1],
                            "measurement": strict_json(row["payload"])
                            if row["payload"]
                            else None,
                            "error": strict_json(row["error"])
                            if row["error"]
                            else None,
                        }
            payload = {
                "schema": 1,
                "id": snapshot_id,
                "windows": windows,
                "observations": [references[key] for key in sorted(references)],
            }
            fingerprint = digest(payload)
            existing = con.execute(
                "SELECT fingerprint FROM snapshots WHERE id=?", (snapshot_id,)
            ).fetchone()
            if existing:
                if existing["fingerprint"] != fingerprint:
                    raise Conflict("snapshot id has different frozen content")
                return {"id": snapshot_id, "fingerprint": fingerprint, "reused": True}
            con.execute(
                "INSERT INTO snapshots VALUES(?,?,?)",
                (snapshot_id, canonical(payload), fingerprint),
            )
            append(
                con,
                "snapshot_created",
                snapshot_id,
                {"fingerprint": fingerprint, "windows": window_ids},
            )
        return {"id": snapshot_id, "fingerprint": fingerprint, "reused": False}

    def get(self, snapshot_id):
        row = self.db.one("SELECT * FROM snapshots WHERE id=?", (snapshot_id,))
        if row is None:
            raise Missing("snapshot not found")
        payload = strict_json(row["payload"])
        if digest(payload) != row["fingerprint"]:
            raise IntegrityError("snapshot content fingerprint differs")
        return {"payload": payload, "fingerprint": row["fingerprint"]}

    def export(self, snapshot_id, destination):
        snapshot = self.get(snapshot_id)
        envelope = {
            "fingerprint": snapshot["fingerprint"],
            "payload": snapshot["payload"],
        }
        return atomic_write(destination, (canonical(envelope) + "\n").encode("utf-8"))

    def csv(self, snapshot_id):
        snapshot = self.get(snapshot_id)
        output = io.StringIO(newline="")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(
            [
                "window_set",
                "generation",
                "start",
                "end",
                "unit",
                "count",
                "mean",
                "minimum",
                "maximum",
                "stddev",
                "uncertainty",
            ]
        )
        for group in snapshot["payload"]["windows"]:
            for window in group["windows"]:
                stats = window["statistics"]
                row = [
                    group["id"],
                    group["generation"],
                    window["start"],
                    window["end"],
                    window["unit"],
                    stats["count"],
                    stats["mean"],
                    stats["minimum"],
                    stats["maximum"],
                    stats["stddev"],
                    stats["uncertainty"],
                ]
                writer.writerow(row)
        return output.getvalue()

    def compare(self, left_id, right_id):
        left = self.get(left_id)["payload"]
        right = self.get(right_id)["payload"]
        before = {row["id"]: row for row in left["observations"]}
        after = {row["id"]: row for row in right["observations"]}
        return {
            "left": left_id,
            "right": right_id,
            "added": sorted(after.keys() - before.keys()),
            "removed": sorted(before.keys() - after.keys()),
            "changed": [
                {
                    "id": key,
                    "before_revision": before[key]["revision"],
                    "after_revision": after[key]["revision"],
                }
                for key in sorted(before.keys() & after.keys())
                if before[key] != after[key]
            ],
        }


def atomic_write(destination, payload):
    destination = Path(destination)
    if destination.exists() or destination.is_symlink():
        raise Conflict("export destination already exists")
    if not destination.parent.is_dir():
        raise InputError("export parent directory does not exist")
    fd, temporary = tempfile.mkstemp(prefix=".metrology-", dir=destination.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, destination)
        except FileExistsError as exc:
            raise Conflict("export destination appeared concurrently") from exc
        return {"path": str(destination), "bytes": len(payload)}
    finally:
        Path(temporary).unlink(missing_ok=True)


def verify_export(path):
    raw = Path(path).read_bytes()
    if len(raw) > 64000000:
        raise InputError("snapshot exceeds supported size")
    try:
        value = strict_json(raw.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise InputError("snapshot must be UTF-8") from exc
    if not isinstance(value, dict) or set(value) != {"payload", "fingerprint"}:
        raise InputError("invalid snapshot envelope")
    if digest(value["payload"]) != value["fingerprint"]:
        raise IntegrityError("export fingerprint mismatch")
    payload = value["payload"]
    if not isinstance(payload, dict) or payload.get("schema") != 1:
        raise InputError("unsupported snapshot schema")
    return {
        "id": payload["id"],
        "fingerprint": value["fingerprint"],
        "windows": len(payload["windows"]),
        "observations": len(payload["observations"]),
    }
