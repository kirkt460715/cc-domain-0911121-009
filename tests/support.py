from metrology.service import Service
from metrology.ingress.readers import InputRow
from metrology.observations import Observation

START = "2026-11-11T00:00:00Z"
END = "2026-11-11T00:01:00Z"
RECEIVED = "2026-11-11T00:02:00Z"


def certificate(**changes):
    value = {
        "id": "c1",
        "instrument": "i1",
        "channel": "p",
        "valid_from": "2026-01-01T00:00:00Z",
        "valid_to": "2027-01-01T00:00:00Z",
        "input_unit": "kPa",
        "output_unit": "kPa",
        "slope": "1",
        "intercept": "0",
        "uncertainty": "0",
        "minimum": "0",
        "maximum": "100",
        "issued_at": "2025-12-01T00:00:00Z",
    }
    value.update(changes)
    return value


def observation(index=0, **changes):
    value = {
        "id": "r" + str(index),
        "source": "s1",
        "sequence": index,
        "instrument": "i1",
        "channel": "p",
        "event_time": f"2026-11-11T00:00:{index % 60:02d}Z",
        "value": "10",
        "unit": "kPa",
        "tags": {"lot": "A"},
    }
    value.update(changes)
    return Observation.parse(value)


def rows(*items):
    return [InputRow(index + 1, observation=item) for index, item in enumerate(items)]


def configure(app):
    app.catalog.add_instrument(
        {"id": "i1", "label": "Synthetic instrument", "channels": {"p": "kPa"}}
    )
    app.catalog.add_source("s1")
    app.catalog.add_certificate(certificate())
    app.catalog.add_policy({"id": "q1", "unit": "kPa", "lower": "0", "upper": "20"})
    app.catalog.bind("i1", "p", "q1")
    return app


def application(path=":memory:"):
    return configure(Service(path))


def window(**changes):
    value = {
        "id": "w1",
        "instrument": "i1",
        "channel": "p",
        "start": START,
        "end": END,
        "seconds": 60,
        "minimum_count": 1,
        "sources": ["s1"],
    }
    value.update(changes)
    return value


def published(app):
    app.pipeline.ingest("b1", rows(observation()), received_at=RECEIVED)
    app.catalog.watermark("s1", END)
    app.windows.materialize(window())
    app.windows.seal("w1", 1)
    return app.snapshots.create("snapshot", ["w1"])
