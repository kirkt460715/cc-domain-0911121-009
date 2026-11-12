"""Helpers for building binary protocol frames in gateway tests."""

import json
import struct
import zlib

from metrology.ingress.binary import MAGIC, END_MARKER, PAYLOAD_SIZE_V1


def make_frame(boot_id, sequence, channel, version, millis, value, *, corrupt=False):
    payload = struct.pack("<IHHHqd", boot_id, sequence, channel, version, millis, value)
    head = MAGIC + struct.pack(">H", len(payload))
    crc = zlib.crc32(head + payload) & 0xFFFFFFFF
    if corrupt:
        crc ^= 0x00000001
    return head + payload + struct.pack(">I", crc)


def make_end(counts):
    body = json.dumps({"counts": counts}).encode("utf-8")
    return END_MARKER + struct.pack(">H", len(body)) + body


def chunked(data, sizes):
    chunks = []
    cursor = 0
    pattern = list(sizes)
    index = 0
    while cursor < len(data):
        size = pattern[index % len(pattern)]
        index += 1
        chunks.append(data[cursor : cursor + size])
        cursor += size
    return chunks


def cycling_sizes(pattern):
    return iter(tuple(pattern))


def directory(version=1, unit="degC", *, valid_from="2026-01-01T00:00:00Z",
              valid_until="2027-01-01T00:00:00Z"):
    return {
        "version": version,
        "valid_from": valid_from,
        "valid_until": valid_until,
        "channels": {
            "1": {
                "instrument": "probe1",
                "channel": "temperature",
                "unit": unit,
                "source": "gateway1",
            }
        },
    }
