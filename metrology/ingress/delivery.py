import hashlib
from pathlib import Path
from ..errors import Conflict, InputError, IntegrityError, Missing
from ..values import (
    require_object,
    identifier,
    integer,
    canonical,
    strict_json,
    digest,
    instant,
    time_text,
)
from .readers import read_input
from .mapping import Mapping
from ..ledger.audit import append


class DeliveryManifest:
    def __init__(self, data):
        require_object(
            data,
            {"id", "received_at", "atomic", "files"},
            {"id", "received_at", "files"},
        )
        self.id = identifier(data["id"])
        self.received_at = time_text(instant(data["received_at"]))
        self.atomic = data.get("atomic", False)
        if not isinstance(self.atomic, bool):
            raise InputError("delivery atomic flag must be boolean")
        files = data["files"]
        if not isinstance(files, list) or not 1 <= len(files) <= 128:
            raise InputError("delivery requires one to one hundred twenty-eight files")
        self.files = []
        seen = set()
        batches = set()
        for item in files:
            fields = {
                "path",
                "sha256",
                "bytes",
                "format",
                "batch",
                "mapping",
                "delimiter",
            }
            require_object(item, fields, {"path", "sha256", "bytes", "format", "batch"})
            path = item["path"]
            if not isinstance(path, str) or not path or "\\" in path:
                raise InputError("delivery path must be portable relative text")
            components = path.split("/")
            if any(part in {"", ".", ".."} for part in components) or path.startswith(
                "/"
            ):
                raise InputError("delivery path must stay within its inbox")
            if path in seen:
                raise InputError("duplicate delivery path")
            seen.add(path)
            batch = identifier(item["batch"])
            if batch in batches:
                raise InputError("duplicate delivery batch")
            batches.add(batch)
            checksum = item["sha256"]
            if (
                not isinstance(checksum, str)
                or len(checksum) != 64
                or any(c not in "0123456789abcdef" for c in checksum)
            ):
                raise InputError("delivery checksum must be lowercase SHA256")
            size = integer(item["bytes"], "bytes", 0, 16000000)
            if item["format"] not in {"csv", "jsonl"}:
                raise InputError("delivery file format must be csv or jsonl")
            mapping = Mapping.parse(item["mapping"]) if item.get("mapping") else None
            if item["format"] == "csv" and mapping is None:
                raise InputError("CSV delivery requires mapping")
            delimiter = item.get("delimiter", ",")
            if delimiter not in {",", ";", "\t", "|"}:
                raise InputError("unsupported delivery delimiter")
            self.files.append(
                {
                    "path": path,
                    "sha256": checksum,
                    "bytes": size,
                    "format": item["format"],
                    "batch": batch,
                    "mapping": mapping.as_dict() if mapping else None,
                    "delimiter": delimiter,
                }
            )

    def as_dict(self):
        return {
            "id": self.id,
            "received_at": self.received_at,
            "atomic": self.atomic,
            "files": self.files,
        }

    def inspect(self, inbox):
        root = Path(inbox)
        if root.is_symlink() or not root.is_dir():
            raise InputError("inbox must be a nonsymlink directory")
        root = root.resolve()
        prepared = []
        for item in self.files:
            path = root
            for component in item["path"].split("/"):
                path = path / component
                if path.is_symlink():
                    raise InputError("delivery cannot traverse symbolic links")
            if not path.is_file() or not path.resolve().is_relative_to(root):
                raise InputError(
                    "delivery file unavailable inside inbox", field=item["path"]
                )
            raw = path.read_bytes()
            if (
                len(raw) != item["bytes"]
                or hashlib.sha256(raw).hexdigest() != item["sha256"]
            ):
                raise IntegrityError(
                    "delivery file checksum or size mismatch", field=item["path"]
                )
            try:
                text = raw.decode("utf-8-sig")
            except UnicodeDecodeError as exc:
                raise InputError(
                    "delivery file is not UTF-8", field=item["path"]
                ) from exc
            mapping = Mapping.parse(item["mapping"]) if item["mapping"] else None
            options = (
                {"delimiter": item["delimiter"]} if item["format"] == "csv" else {}
            )
            rows = read_input(text, item["format"], mapping, **options)
            prepared.append((item, rows))
        return prepared


class DeliveryInbox:
    def __init__(self, database, pipeline):
        self.db = database
        self.pipeline = pipeline
        with database.transaction() as con:
            con.execute("""CREATE TABLE IF NOT EXISTS deliveries(id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,
                manifest TEXT NOT NULL,state TEXT NOT NULL,result TEXT)""")
            con.execute("""CREATE TABLE IF NOT EXISTS delivery_files(delivery TEXT NOT NULL,position INTEGER NOT NULL,
                batch TEXT NOT NULL,state TEXT NOT NULL,result TEXT,PRIMARY KEY(delivery,position))""")

    def receive(self, data, inbox, *, maximum_files=None):
        manifest = DeliveryManifest(data)
        fingerprint = digest(manifest.as_dict())
        prepared = manifest.inspect(inbox)
        if maximum_files is not None:
            integer(maximum_files, "maximum_files", 1, 128)
        with self.db.transaction() as con:
            previous = con.execute(
                "SELECT * FROM deliveries WHERE id=?", (manifest.id,)
            ).fetchone()
            if previous:
                if previous["fingerprint"] != fingerprint:
                    raise Conflict("delivery id has a different manifest")
                if previous["state"] == "complete":
                    return {**strict_json(previous["result"]), "reused": True}
            else:
                con.execute(
                    "INSERT INTO deliveries VALUES(?,?,?,'pending',NULL)",
                    (manifest.id, fingerprint, canonical(manifest.as_dict())),
                )
                for position, (item, _) in enumerate(prepared):
                    con.execute(
                        "INSERT INTO delivery_files VALUES(?,?,?,'pending',NULL)",
                        (manifest.id, position, item["batch"]),
                    )
                append(
                    con,
                    "delivery_received",
                    manifest.id,
                    {"files": len(prepared), "fingerprint": fingerprint},
                )
        processed = 0
        for position, (item, rows) in enumerate(prepared):
            if maximum_files is not None and processed >= maximum_files:
                break
            with self.db.transaction() as con:
                state = con.execute(
                    "SELECT state FROM delivery_files WHERE delivery=? AND position=?",
                    (manifest.id, position),
                ).fetchone()["state"]
                if state == "complete":
                    continue
                result = self.pipeline.ingest(
                    item["batch"],
                    rows,
                    received_at=manifest.received_at,
                    atomic=manifest.atomic,
                )
                con.execute(
                    "UPDATE delivery_files SET state='complete',result=? WHERE delivery=? AND position=?",
                    (canonical(result), manifest.id, position),
                )
                append(
                    con,
                    "delivery_file_committed",
                    manifest.id,
                    {"position": position, "batch": item["batch"]},
                )
                processed += 1
        with self.db.transaction() as con:
            states = self.db.rows(
                "SELECT * FROM delivery_files WHERE delivery=? ORDER BY position",
                (manifest.id,),
            )
            complete = all(row["state"] == "complete" for row in states)
            result = {
                "id": manifest.id,
                "state": "complete" if complete else "pending",
                "reused": False,
                "committed_files": sum(row["state"] == "complete" for row in states),
                "files": [
                    {
                        "batch": row["batch"],
                        "state": row["state"],
                        "result": strict_json(row["result"]) if row["result"] else None,
                    }
                    for row in states
                ],
            }
            con.execute(
                "UPDATE deliveries SET state=?,result=? WHERE id=?",
                (result["state"], canonical(result), manifest.id),
            )
            if complete:
                append(con, "delivery_completed", manifest.id, {"files": len(states)})
        return result

    def status(self, delivery_id):
        row = self.db.one("SELECT * FROM deliveries WHERE id=?", (delivery_id,))
        if row is None:
            raise Missing("delivery not found")
        files = self.db.rows(
            "SELECT position,batch,state,result FROM delivery_files WHERE delivery=? ORDER BY position",
            (delivery_id,),
        )
        for item in files:
            item["result"] = strict_json(item["result"]) if item["result"] else None
        return {
            "id": delivery_id,
            "state": row["state"],
            "fingerprint": row["fingerprint"],
            "files": files,
        }

    def pending(self):
        return [
            self.status(row["id"])
            for row in self.db.rows(
                "SELECT id FROM deliveries WHERE state='pending' ORDER BY id"
            )
        ]
