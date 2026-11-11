from dataclasses import dataclass
from decimal import Decimal, localcontext
from ..errors import InputError, StateError, Conflict, Missing
from ..values import (
    identifier,
    instant,
    time_text,
    integer,
    canonical,
    strict_json,
    digest,
    decimal_text,
    epoch_microseconds,
    from_microseconds,
    require_object,
)
from ..observations import Measurement
from ..ledger.audit import append


@dataclass(frozen=True)
class WindowSpec:
    id: str
    instrument: str
    channel: str
    start: object
    end: object
    seconds: int
    minimum_count: int
    sources: tuple

    @classmethod
    def parse(cls, data):
        fields = {
            "id",
            "instrument",
            "channel",
            "start",
            "end",
            "seconds",
            "minimum_count",
            "sources",
        }
        require_object(data, fields, fields - {"minimum_count"})
        start, end = instant(data["start"]), instant(data["end"])
        seconds = integer(data["seconds"], "seconds", 1, 86400 * 31)
        if end <= start:
            raise InputError("window range must increase")
        if (epoch_microseconds(end) - epoch_microseconds(start)) % (seconds * 1000000):
            raise InputError("window range must contain whole intervals")
        count = (epoch_microseconds(end) - epoch_microseconds(start)) // (
            seconds * 1000000
        )
        if count > 10000:
            raise InputError("at most ten thousand windows per materialization")
        sources = data["sources"]
        if not isinstance(sources, list) or not sources or len(sources) > 128:
            raise InputError("window sources must be a nonempty bounded list")
        if len(set(sources)) != len(sources):
            raise InputError("duplicate window source")
        return cls(
            identifier(data["id"]),
            identifier(data["instrument"]),
            identifier(data["channel"]),
            start,
            end,
            seconds,
            integer(data.get("minimum_count", 1), "minimum_count", 1, 1000000),
            tuple(sorted(identifier(source) for source in sources)),
        )

    def as_dict(self):
        return {
            "id": self.id,
            "instrument": self.instrument,
            "channel": self.channel,
            "start": time_text(self.start),
            "end": time_text(self.end),
            "seconds": self.seconds,
            "minimum_count": self.minimum_count,
            "sources": list(self.sources),
        }

    def intervals(self):
        start, end = epoch_microseconds(self.start), epoch_microseconds(self.end)
        size = self.seconds * 1000000
        return [
            (from_microseconds(left), from_microseconds(left + size))
            for left in range(start, end, size)
        ]


def statistics(values):
    if not values:
        return {
            "count": 0,
            "mean": None,
            "minimum": None,
            "maximum": None,
            "stddev": None,
            "uncertainty": None,
        }
    with localcontext() as ctx:
        ctx.prec = 40
        numbers = [value.value for value in values]
        count = Decimal(len(numbers))
        mean = sum(numbers, Decimal(0)) / count
        variance = sum(((value - mean) ** 2 for value in numbers), Decimal(0)) / count
        uncertainty = (
            sum((value.uncertainty**2 for value in values), Decimal(0)).sqrt() / count
        )
        return {
            "count": len(numbers),
            "mean": decimal_text(mean),
            "minimum": decimal_text(min(numbers)),
            "maximum": decimal_text(max(numbers)),
            "stddev": decimal_text(variance.sqrt()),
            "uncertainty": decimal_text(uncertainty),
        }


class WindowEngine:
    def __init__(self, database, catalog):
        self.db = database
        self.catalog = catalog

    def materialize(self, specification, expected_generation=None):
        spec = WindowSpec.parse(specification)
        with self.db.transaction() as con:
            self.catalog.channel(spec.instrument, spec.channel)
            source_states = {
                source: self.catalog.source(source) for source in spec.sources
            }
            old = con.execute("SELECT * FROM windows WHERE id=?", (spec.id,)).fetchone()
            if old:
                if old["specification"] != canonical(spec.as_dict()):
                    raise Conflict("window identity cannot change specification")
                if old["sealed"]:
                    raise StateError("sealed windows cannot be rebuilt")
                if (
                    expected_generation is not None
                    and expected_generation != old["generation"]
                ):
                    raise Conflict("window generation changed")
                generation = old["generation"] + 1
            else:
                if expected_generation not in (None, 0):
                    raise Conflict("window does not have expected generation")
                generation = 1
            rows = self.db.rows(
                """SELECT o.id,o.source,o.event_time,r.payload,r.error,r.revision
                FROM observations o JOIN results r ON r.observation=o.id WHERE r.active=1
                AND o.instrument=? AND o.channel=? AND o.event_time>=? AND o.event_time<? ORDER BY o.event_time,o.id""",
                (
                    spec.instrument,
                    spec.channel,
                    time_text(spec.start),
                    time_text(spec.end),
                ),
            )
            rows = [row for row in rows if row["source"] in source_states]
            items = []
            cursor = 0
            for left, right in spec.intervals():
                bucket = []
                while (
                    cursor < len(rows) and instant(rows[cursor]["event_time"]) < right
                ):
                    bucket.append(rows[cursor])
                    cursor += 1
                accepted, rejected, failed = [], [], []
                for row in bucket:
                    if row["error"]:
                        failed.append(row["id"])
                        continue
                    measurement = Measurement.restore(strict_json(row["payload"]))
                    (accepted if measurement.accepted else rejected).append(measurement)
                units = {item.unit for item in accepted}
                if len(units) > 1:
                    raise StateError(
                        "mixed policy units require replay before aggregation"
                    )
                closed = all(
                    state["watermark"] is not None
                    and epoch_microseconds(instant(state["watermark"]))
                    - state["allowed_lateness"] * 1000000
                    >= epoch_microseconds(right)
                    for state in source_states.values()
                )
                item = {
                    "start": time_text(left),
                    "end": time_text(right),
                    "state": "final" if closed else "open",
                    "sufficient": len(accepted) >= spec.minimum_count,
                    "unit": next(iter(units))
                    if units
                    else self.catalog.bound_policy(spec.instrument, spec.channel).unit,
                    "statistics": statistics(accepted),
                    "accepted": [m.observation.id for m in accepted],
                    "rejected": [m.observation.id for m in rejected],
                    "failed": failed,
                    "lineage": [
                        {"id": row["id"], "revision": row["revision"]} for row in bucket
                    ],
                }
                items.append(item)
            result = {
                "id": spec.id,
                "generation": generation,
                "specification": spec.as_dict(),
                "windows": items,
                "source_watermarks": {
                    key: row["watermark"] for key, row in source_states.items()
                },
                "sealed": False,
            }
            con.execute(
                "INSERT INTO windows VALUES(?,?,?,?,0) ON CONFLICT(id) DO UPDATE SET generation=excluded.generation,payload=excluded.payload",
                (spec.id, canonical(spec.as_dict()), generation, canonical(result)),
            )
            append(
                con,
                "windows_materialized",
                spec.id,
                {
                    "generation": generation,
                    "count": len(items),
                    "fingerprint": digest(result),
                },
            )
        return result

    def get(self, window_id):
        row = self.db.one("SELECT * FROM windows WHERE id=?", (window_id,))
        if row is None:
            raise Missing("window materialization not found")
        return strict_json(row["payload"])

    def seal(self, window_id, expected_generation):
        with self.db.transaction() as con:
            result = self.get(window_id)
            if result["generation"] != expected_generation:
                raise Conflict("stale window generation")
            if result["sealed"]:
                return result
            if any(
                item["state"] != "final" or not item["sufficient"]
                for item in result["windows"]
            ):
                raise StateError("all windows must be final and sufficiently populated")
            for window in result["windows"]:
                for reference in window["lineage"]:
                    current = con.execute(
                        "SELECT revision FROM results WHERE observation=? AND active=1",
                        (reference["id"],),
                    ).fetchone()
                    if current is None or current["revision"] != reference["revision"]:
                        raise Conflict("window lineage changed; rebuild before sealing")
            result["sealed"] = True
            con.execute(
                "UPDATE windows SET sealed=1,payload=? WHERE id=?",
                (canonical(result), window_id),
            )
            append(
                con,
                "windows_sealed",
                window_id,
                {"generation": expected_generation, "fingerprint": digest(result)},
            )
        return result
