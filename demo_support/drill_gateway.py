"""End-to-end drill for direct binary gateway collection.

The drill synthesizes every failure mode named in the protocol exercise and
runs it through the real metrology processing chain:

1. legal frames cut into arbitrary byte chunks;
2. a CRC-corrupt frame that must create an explicit resynchronization gap;
3. a gateway restart (new boot_id) with colliding sequence numbers;
4. sequence wraparound from 65535 to 0;
5. an exact retransmission and a same-sequence/different-payload conflict;
6. a frame whose directory version is unknown, registered only later;
7. a stream whose END counts do not match the received frames.

Run with isolated mode so the current directory is never imported:
    python -I demo_support/drill_gateway.py
"""

import json
import struct
import sys
import tempfile
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from metrology.ingress.binary import MAGIC, END_MARKER  # noqa: E402
from metrology.service import Service  # noqa: E402
from metrology.ingress.gateway import GatewayCollector  # noqa: E402
from metrology.ingress.directories import DirectoryDeferred  # noqa: E402

# 2026-11-11T00:00:00Z and a second sampling instant inside the same window.
T0 = 1794355200000
T1 = T0 + 60000
# 2027-01-01T00:00:00Z, covered by the second calibration certificate.
T2027 = 1798761600000


def frame(boot, sequence, channel, version, millis, value, *, corrupt=False):
    payload = struct.pack("<IHHHqd", boot, sequence, channel, version, millis, value)
    head = MAGIC + struct.pack(">H", len(payload))
    crc = zlib.crc32(head + payload) & 0xFFFFFFFF
    if corrupt:
        crc ^= 1
    return head + payload + struct.pack(">I", crc)


def end_marker(counts):
    body = json.dumps({"counts": counts}).encode("utf-8")
    return END_MARKER + struct.pack(">H", len(body)) + body


def arbitrary_chunks(data, pattern=(1, 2, 1, 3, 1, 7, 2, 5)):
    """Slice the stream at deliberately irregular byte boundaries."""
    chunks = []
    cursor = 0
    index = 0
    while cursor < len(data):
        size = pattern[index % len(pattern)]
        index += 1
        chunks.append(data[cursor : cursor + size])
        cursor += size
    return chunks


def configure(app):
    app.catalog.add_instrument(
        {"id": "probe1", "label": "Drill probe", "channels": {"temperature": "degC"}}
    )
    app.catalog.add_policy(
        {"id": "temp", "unit": "degC", "lower": "-50", "upper": "100"}
    )
    app.catalog.bind("probe1", "temperature", "temp")
    app.catalog.add_certificate(
        {
            "id": "cert-2026",
            "instrument": "probe1",
            "channel": "temperature",
            "valid_from": "2026-01-01T00:00:00Z",
            "valid_to": "2027-01-01T00:00:00Z",
            "input_unit": "degC",
            "output_unit": "degC",
            "slope": "1",
            "intercept": "0",
            "uncertainty": "0.1",
            "minimum": "-100",
            "maximum": "200",
            "issued_at": "2025-12-01T00:00:00Z",
        }
    )
    app.catalog.add_certificate(
        {
            "id": "cert-2027",
            "instrument": "probe1",
            "channel": "temperature",
            "valid_from": "2027-01-01T00:00:00Z",
            "valid_to": "2028-01-01T00:00:00Z",
            "input_unit": "degC",
            "output_unit": "degC",
            "slope": "2",
            "intercept": "0",
            "uncertainty": "0.1",
            "minimum": "-100",
            "maximum": "200",
            "issued_at": "2026-12-01T00:00:00Z",
        }
    )
    app.directories.register(
        {
            "version": 1,
            "valid_from": "2026-01-01T00:00:00Z",
            "valid_until": "2027-01-01T00:00:00Z",
            "channels": {
                "1": {
                    "instrument": "probe1",
                    "channel": "temperature",
                    "unit": "degC",
                    "source": "gateway1",
                }
            },
        }
    )


def main():
    with tempfile.TemporaryDirectory(prefix="gateway-drill-") as directory:
        database = Path(directory) / "drill.sqlite3"
        with Service(database) as app:
            configure(app)
            collector: GatewayCollector = app.gateway

            # Stream layout:
            # boot 7: seq 65534, 65535 (wrap boundary), then 0, 1;
            #         one CRC-bad frame and one exact retransmission;
            # boot 8: seq 0 (restart) carrying a different value;
            # boot 9: seq 0 referencing unknown directory version 2;
            # plus a same-sequence/different-payload conflict on boot 7.
            good_wrap_65534 = frame(7, 65534, 1, 1, T0, 20.0)
            good_wrap_65535 = frame(7, 65535, 1, 1, T0, 21.0)
            good_wrap_0 = frame(7, 0, 1, 1, T1, 22.0)
            good_wrap_1 = frame(7, 1, 1, 1, T1, 23.0)
            bad_frame = frame(7, 2, 1, 1, T1, 24.0, corrupt=True)
            restart_frame = frame(8, 0, 1, 1, T1, 25.0)
            conflict_frame = frame(7, 65534, 1, 1, T0, 99.0)
            future_frame = frame(9, 0, 1, 2, T2027, 30.0)
            stream = b"".join(
                [
                    good_wrap_65534,
                    good_wrap_65535,
                    good_wrap_0,
                    good_wrap_1,
                    good_wrap_0,  # exact retransmission
                    bad_frame,
                    restart_frame,
                    conflict_frame,
                    future_frame,
                    # Declares more frames than survive: bad frame is lost.
                    end_marker({"7:1": 6, "8:1": 1, "9:1": 1}),
                ]
            )

            status = collector.collect(
                "drill-1",
                "gateway1",
                arbitrary_chunks(stream),
                received_at="2026-11-11T02:00:00Z",
            )
            summary = status["summary"]

            assertions = []

            def check(name, condition, detail):
                assertions.append((name, bool(condition), detail))

            check("session completed with trailer", status["state"] == "complete", status["state"])
            check(
                "all nine frame receptions counted including the CRC loss",
                summary["frames_seen"] == 9,
                summary["frames_seen"],
            )
            check(
                "eight frames decoded past the CRC check",
                summary["frames_received"] == 8,
                summary["frames_received"],
            )
            check(
                "one exact retransmission",
                summary["retransmissions"] == 1,
                summary["retransmissions"],
            )
            check("one payload conflict", summary["conflicts"] == 1, summary["conflicts"])
            check(
                "one frame deferred on unknown directory",
                summary["deferred"] == 1,
                summary["deferred"],
            )
            check(
                "CRC loss reconciled against END counts",
                summary["missing"]
                == {"7:1": {"declared": 6, "received": 4, "missing": 2}},
                summary["missing"],
            )
            check(
                "boot 8 restart fully received",
                summary["unique_frames"].get("8:1") == 1,
                summary["unique_frames"],
            )

            event_kinds = [event["kind"] for event in collector.events("drill-1")]
            check("CRC failure is explicit evidence", "crc_failure" in event_kinds, event_kinds)

            stored = collector.frames("drill-1")
            statuses = [item["status"] for item in stored]
            check(
                "frame ledger covers accepted/retransmitted/conflict/deferred",
                {"accepted", "retransmitted", "conflict", "deferred"} <= set(statuses),
                statuses,
            )
            # Wrap generation must distinguish the two boot-7 sequence-0 frames.
            boot7 = [item for item in stored if item["boot_id"] == 7]
            generations = {
                item["sequence"]: item["generation"] for item in boot7
            }
            check(
                "wraparound advances the generation (65535 -> g1:0)",
                generations.get(65535) == 0 and generations.get(0) == 1,
                generations,
            )
            # Restart means boot 8 sequence 0 is generation 0 on a new source.
            boot8 = next(item for item in stored if item["boot_id"] == 8)
            check("restart resets generation per boot", boot8["generation"] == 0, boot8)

            observations = app.reports.query(limit=100)["items"]
            # Accepted unique frames: four boot-7 wrap frames plus the restart.
            accepted = [item for item in observations if item["status"] == "accepted"]
            check("five unique observations accepted", len(accepted) == 5, len(accepted))
            global_sequences = sorted(item["sequence"] for item in accepted)
            check(
                "global sequences are monotonic across the wrap",
                global_sequences == [0, 65534, 65535, 65536, 65537],
                global_sequences,
            )
            sources = {item["source"] for item in accepted}
            check(
                "boot identity splits the gateway into sources",
                sources == {"gateway1.boot7", "gateway1.boot8"},
                sorted(sources),
            )
            # The conflict frame is quarantined rather than overwriting data.
            check(
                "conflict is quarantined",
                app.reports.batch("drill-1")["quarantines"] != [],
                "quarantine present",
            )

            # The unknown-directory frame must not have been force-calibrated.
            check("no boot-9 observation before registration",
                  all(not item["source"].endswith("boot9") for item in observations),
                  "deferred")
            try:
                app.directories.resolve(2, 1, "2027-01-01T00:00:00Z")
                check("unknown directory still deferred pre-registration", False, "raised")
            except DirectoryDeferred:
                check("unknown directory still deferred pre-registration", True, "deferred")

            # Register the missing version, then reprocess the isolated frames.
            app.directories.register(
                {
                    "version": 2,
                    "valid_from": "2027-01-01T00:00:00Z",
                    "valid_until": "2028-01-01T00:00:00Z",
                    "channels": {
                        "1": {
                            "instrument": "probe1",
                            "channel": "temperature",
                            "unit": "degC",
                            "source": "gateway1",
                        }
                    },
                }
            )
            reprocessed = collector.reprocess_session(
                "drill-1", received_at="2026-11-11T03:00:00Z"
            )
            check("deferred frame reprocessed once", reprocessed["reprocessed"] == 1, reprocessed)
            later = app.reports.query(limit=100)["items"]
            boot9 = next(item for item in later if item["source"] == "gateway1.boot9")
            check(
                "2027 frame uses the 2027 calibration (slope 2)",
                boot9["measurement"]["certificate"] == "cert-2027"
                and boot9["measurement"]["corrected_value"] == "60",
                boot9["measurement"],
            )
            check(
                "2026 frames kept the 2026 calibration (slope 1)",
                all(
                    item["measurement"]["certificate"] == "cert-2026"
                    for item in later
                    if item["source"] == "gateway1.boot7"
                ),
                "cert-2026",
            )

            # Isolated single-frame reprocessing entry.
            single = frame(8, 5, 1, 1, T1, 26.0)
            isolated = collector.reprocess_frame(
                "gateway1",
                single,
                batch_id="isolated-1",
                received_at="2026-11-11T03:30:00Z",
            )
            check("isolated frame entry ingests one row", isolated["inserted"] == 1, isolated)

            integrity = app.inspector.check()
            check("audit and invariants intact", integrity["invariants"] == "ok", integrity)

            width = max(len(name) for name, _, _ in assertions)
            failed = 0
            for name, ok, detail in assertions:
                print(f"{'PASS' if ok else 'FAIL'}  {name.ljust(width)}  {detail}")
                if not ok:
                    failed += 1
            print(
                json.dumps(
                    {
                        "session_state": status["state"],
                        "summary": summary,
                        "reprocessed": reprocessed,
                        "observations": app.reports.overview()["observations"],
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            if failed:
                raise SystemExit(f"gateway drill failed on {failed} assertion(s)")
            print("网关二进制采集演练全部通过")


if __name__ == "__main__":
    main()
