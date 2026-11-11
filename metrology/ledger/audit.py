from ..errors import IntegrityError
from ..values import canonical, digest, strict_json, identifier, integer

ZERO = "0" * 64


def append(connection, kind, object_id, payload):
    identifier(kind, "audit_kind")
    identifier(object_id, "audit_object")
    row = connection.execute(
        "SELECT sequence,hash FROM audit ORDER BY sequence DESC LIMIT 1"
    ).fetchone()
    previous = row["hash"] if row else ZERO
    sequence = row["sequence"] + 1 if row else 1
    body = {
        "sequence": sequence,
        "kind": kind,
        "object_id": object_id,
        "payload": payload,
        "previous": previous,
    }
    hashed = digest(body)
    connection.execute(
        "INSERT INTO audit(sequence,kind,object_id,payload,previous,hash) VALUES(?,?,?,?,?,?)",
        (sequence, kind, object_id, canonical(payload), previous, hashed),
    )
    return hashed


def verify(database):
    rows = database.rows("SELECT * FROM audit ORDER BY sequence")
    previous = ZERO
    for expected, row in enumerate(rows, 1):
        if row["sequence"] != expected or row["previous"] != previous:
            raise IntegrityError(
                "audit chain discontinuity", details={"sequence": expected}
            )
        body = {
            "sequence": expected,
            "kind": row["kind"],
            "object_id": row["object_id"],
            "payload": strict_json(row["payload"]),
            "previous": previous,
        }
        if digest(body) != row["hash"]:
            raise IntegrityError(
                "audit content hash mismatch", details={"sequence": expected}
            )
        previous = row["hash"]
    return {"entries": len(rows), "head": previous}


def history(database, object_id=None, after=0, limit=100):
    integer(after, "after")
    integer(limit, "limit", 1, 1000)
    if object_id is not None:
        identifier(object_id)
        rows = database.rows(
            "SELECT * FROM audit WHERE sequence>? AND object_id=? ORDER BY sequence LIMIT ?",
            (after, object_id, limit),
        )
    else:
        rows = database.rows(
            "SELECT * FROM audit WHERE sequence>? ORDER BY sequence LIMIT ?",
            (after, limit),
        )
    for row in rows:
        row["payload"] = strict_json(row["payload"])
    return rows
