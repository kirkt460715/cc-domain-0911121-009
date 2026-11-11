import json
import tempfile
from pathlib import Path
from .service import Service
from .ingress.readers import read_jsonl
from .output.snapshots import verify_export


def demonstration():
    with tempfile.TemporaryDirectory(prefix="liangheng-") as directory:
        root = Path(directory)
        with Service(root / "lab.sqlite3") as app:
            app.configuration.apply(
                {
                    "id": "demo-config",
                    "instruments": [
                        {
                            "id": "press-1",
                            "label": "Synthetic pressure instrument",
                            "channels": {"p": "kPa"},
                        }
                    ],
                    "sources": [{"id": "bench", "allowed_lateness": 0}],
                    "certificates": [
                        {
                            "id": "cert-1",
                            "instrument": "press-1",
                            "channel": "p",
                            "valid_from": "2026-01-01T00:00:00Z",
                            "valid_to": "2027-01-01T00:00:00Z",
                            "input_unit": "kPa",
                            "output_unit": "kPa",
                            "slope": "1",
                            "intercept": ".1",
                            "uncertainty": ".01",
                            "minimum": "0",
                            "maximum": "100",
                            "issued_at": "2025-12-01T00:00:00Z",
                        }
                    ],
                    "policies": [
                        {
                            "id": "q1",
                            "unit": "kPa",
                            "lower": "0",
                            "upper": "20",
                            "required_tags": ["lot"],
                        }
                    ],
                    "bindings": [
                        {"instrument": "press-1", "channel": "p", "policy": "q1"}
                    ],
                }
            )
            values = [
                {
                    "id": f"r{i}",
                    "source": "bench",
                    "sequence": i,
                    "instrument": "press-1",
                    "channel": "p",
                    "event_time": f"2026-11-11T00:00:{i * 10:02d}Z",
                    "value": str(10 + i),
                    "unit": "kPa",
                    "uncertainty": "0",
                    "tags": {"lot": "synthetic-A"},
                }
                for i in range(3)
            ]
            rows = read_jsonl("\n".join(json.dumps(value) for value in values))
            ingestion = app.pipeline.ingest(
                "demo-batch", rows, received_at="2026-11-11T00:01:00Z"
            )
            duplicate = app.pipeline.ingest(
                "demo-batch", rows, received_at="2026-11-11T00:01:00Z"
            )
            app.catalog.watermark("bench", "2026-11-11T00:01:00Z")
            window = app.windows.materialize(
                {
                    "id": "minute",
                    "instrument": "press-1",
                    "channel": "p",
                    "start": "2026-11-11T00:00:00Z",
                    "end": "2026-11-11T00:01:00Z",
                    "seconds": 60,
                    "sources": ["bench"],
                }
            )
            app.windows.seal("minute", window["generation"])
            snapshot = app.snapshots.create("release-1", ["minute"])
            app.snapshots.export("release-1", root / "snapshot.json")
            verification = verify_export(root / "snapshot.json")
            integrity = app.inspector.check()
            assert ingestion["accepted"] == 3
            assert duplicate["reused"] is True
            assert window["windows"][0]["statistics"]["mean"] == "11.1"
            assert verification["observations"] == 3
            return {
                "ingestion": ingestion,
                "duplicate_reused": duplicate["reused"],
                "mean": window["windows"][0]["statistics"]["mean"],
                "snapshot": snapshot,
                "export_verified": verification,
                "integrity": integrity,
            }
