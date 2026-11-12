"""Codec for the binary observation exchange protocol v1.

A frame is ``A5 5A``, a two-byte big-endian payload length, the payload and a
four-byte big-endian CRC32 (zlib) covering magic, length and payload.  The
stream may end with an ``END!`` marker carrying a JSON trailer.

The parser is deliberately byte-oriented: HTTP chunk boundaries may fall at
any byte and the magic bytes may legitimately appear inside a payload.  Every
uninterpretable byte range is reported as an explicit resynchronization gap,
so callers never silently lose bytes.
"""

import datetime as dt
import json
import math
import struct
import zlib
from dataclasses import dataclass, field

from ..errors import InputError
from ..values import time_text

MAGIC = b"\xa5\x5a"
END_MARKER = b"END!"
HEADER_SIZE = 4
CRC_SIZE = 4
MAX_FRAME_BYTES = 4096
PAYLOAD_SIZE_V1 = 26
SEQUENCE_MODULUS = 65536
MAX_GAP_PREVIEW = 32
PAYLOAD_STRUCT = struct.Struct("<IHHHqd")


@dataclass(frozen=True)
class Frame:
    """One structurally valid frame with a verified CRC."""

    index: int
    offset: int
    length: int
    raw: bytes
    payload: object


@dataclass(frozen=True)
class FramePayload:
    boot_id: int
    sequence: int
    channel: int
    directory_version: int
    sampled_at: object
    raw_value: object

    def as_dict(self):
        return {
            "boot_id": self.boot_id,
            "sequence": self.sequence,
            "channel": self.channel,
            "directory_version": self.directory_version,
            "sampled_at": time_text(self.sampled_at),
            "raw_value": self.raw_value,
        }


@dataclass(frozen=True)
class StreamEvent:
    kind: str
    offset: int
    evidence: dict = field(default_factory=dict)


def decode_payload(raw, offset):
    if len(raw) != PAYLOAD_SIZE_V1:
        return None, StreamEvent(
            "unsupported_payload",
            offset,
            {"length": len(raw), "supported": [PAYLOAD_SIZE_V1]},
        )
    boot_id, sequence, channel, version, millis, value = PAYLOAD_STRUCT.unpack(raw)
    if not math.isfinite(value):
        return None, StreamEvent(
            "invalid_payload", offset, {"reason": "raw_value is not finite"}
        )
    try:
        sampled_at = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(
            milliseconds=millis
        )
    except OverflowError:
        return None, StreamEvent(
            "invalid_payload", offset, {"reason": "sampling time out of range"}
        )
    if sampled_at.year < 1970 or sampled_at.year > 2200:
        return None, StreamEvent(
            "invalid_payload", offset, {"reason": "sampling time out of range"}
        )
    return FramePayload(boot_id, sequence, channel, version, sampled_at, value), None


def parse_counts(trailer):
    if not isinstance(trailer, dict) or not isinstance(trailer.get("counts"), dict):
        raise InputError("END marker must contain a counts object")
    counts = {}
    for key, value in trailer["counts"].items():
        boot, separator, channel = str(key).partition(":")
        if not separator or not boot.isascii() or not boot.isdecimal():
            raise InputError("END counts key must be boot:channel")
        if not channel.isascii() or not channel.isdecimal():
            raise InputError("END counts key must be boot:channel")
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise InputError("END counts must be nonnegative integers")
        counts[(int(boot), int(channel))] = value
    return counts


class FrameParser:
    """Incremental parser with bounded buffering and explicit resync gaps."""

    def __init__(
        self,
        *,
        max_buffered_bytes=MAX_FRAME_BYTES * 4,
        gap_preview=MAX_GAP_PREVIEW,
    ):
        if max_buffered_bytes < MAX_FRAME_BYTES + HEADER_SIZE:
            raise InputError("buffer bound must hold the largest legal frame")
        self._buffer = bytearray()
        self._consumed = 0
        self._index = 0
        self.max_buffered_bytes = max_buffered_bytes
        self.gap_preview = gap_preview
        self.finished = False
        self.counts = None

    @property
    def offset(self):
        """Absolute stream offset of the first buffered byte."""
        return self._consumed

    def feed(self, chunk):
        if self.finished:
            raise InputError("stream already finalized by an END marker")
        if not isinstance(chunk, (bytes, bytearray)):
            raise InputError("stream chunks must be bytes")
        self._buffer.extend(chunk)
        return list(self._drain())

    def finish(self):
        if self.finished:
            return []
        self.finished = True
        events = list(self._drain())
        if self._buffer:
            events.append(
                StreamEvent(
                    "resync_gap",
                    self._consumed,
                    self._gap_evidence(
                        len(self._buffer), "stream ended without a complete frame"
                    ),
                )
            )
            self._consumed += len(self._buffer)
            self._buffer.clear()
        return events

    def _gap_evidence(self, length, reason):
        preview = bytes(self._buffer[: min(length, self.gap_preview)])
        evidence = {"bytes": length, "reason": reason, "preview_hex": preview.hex()}
        if length > self.gap_preview:
            evidence["preview_truncated"] = True
        return evidence

    def _emit_gap(self, length, reason):
        event = StreamEvent("resync_gap", self._consumed, self._gap_evidence(length, reason))
        del self._buffer[:length]
        self._consumed += length
        return event

    def _drain(self):
        # Loop invariant: every iteration either returns or removes at least
        # one byte, which makes the resynchronization process terminating.
        while self._buffer:
            # Enforce the bound by discarding everything that cannot be the
            # start of a frame or END marker while retaining a short tail
            # prefix that may complete on the next chunk.
            if len(self._buffer) > self.max_buffered_bytes:
                magic_at = self._buffer.find(MAGIC)
                end_at = self._buffer.find(END_MARKER)
                limit = self.max_buffered_bytes - HEADER_SIZE + 1
                candidates = [index for index in (magic_at, end_at) if 0 <= index < limit]
                if candidates:
                    discard = min(candidates)
                else:
                    discard = len(self._buffer) - HEADER_SIZE + 1
                # A complete legal frame plus its header always fits, so a
                # marker at or beyond the limit means buffered frame noise.
                discard = max(discard, 1)
                if discard > 0:
                    yield self._emit_gap(discard, "buffer bound reached without marker")
                    continue
            if self._buffer.startswith(END_MARKER):
                event = self._drain_end_marker()
                if event is not None:
                    yield event
                    return
                return
            position = self._buffer.find(MAGIC)
            end_position = self._buffer.find(END_MARKER)
            if end_position >= 0 and (position < 0 or end_position < position):
                if end_position > 0:
                    yield self._emit_gap(
                        end_position, "non-frame bytes before END marker"
                    )
                    continue
                return
            if position > 0:
                yield self._emit_gap(position, "non-frame bytes before magic")
                continue
            if position < 0:
                # Retain a possible partial magic / END prefix at the tail.
                keep = self._tail_prefix_len()
                discard = len(self._buffer) - keep
                if discard:
                    yield self._emit_gap(discard, "no frame marker in buffered bytes")
                return
            if len(self._buffer) < HEADER_SIZE:
                return
            length = struct.unpack(">H", self._buffer[2:4])[0]
            frame_size = HEADER_SIZE + length + CRC_SIZE
            if frame_size > MAX_FRAME_BYTES:
                yield StreamEvent(
                    "frame_too_long",
                    self._consumed,
                    {"declared_length": length, "maximum": MAX_FRAME_BYTES},
                )
                del self._buffer[:HEADER_SIZE]
                self._consumed += HEADER_SIZE
                continue
            if len(self._buffer) < frame_size:
                return
            frame_start = self._consumed
            raw_frame = bytes(self._buffer[:frame_size])
            del self._buffer[:frame_size]
            self._consumed += frame_size
            expected = struct.unpack(
                ">I", raw_frame[HEADER_SIZE + length : frame_size]
            )[0]
            actual = zlib.crc32(raw_frame[: HEADER_SIZE + length]) & 0xFFFFFFFF
            if actual != expected:
                yield StreamEvent(
                    "crc_failure",
                    frame_start,
                    {
                        "bytes": frame_size,
                        "declared_length": length,
                        "expected_crc": expected,
                        "actual_crc": actual,
                        "raw_hex": raw_frame.hex(),
                    },
                )
                continue
            payload, problem = decode_payload(
                raw_frame[HEADER_SIZE : HEADER_SIZE + length], frame_start
            )
            if problem is not None:
                yield problem
                continue
            frame = Frame(self._index, frame_start, frame_size, raw_frame, payload)
            self._index += 1
            yield StreamEvent("frame", frame_start, {"frame": frame})

    def _tail_prefix_len(self):
        keep = 0
        for size in range(1, min(len(END_MARKER), len(self._buffer)) + 1):
            if END_MARKER.startswith(bytes(self._buffer[-size:])):
                keep = max(keep, size)
        if self._buffer[-1] == MAGIC[0]:
            keep = max(keep, 1)
        return keep

    def _drain_end_marker(self):
        available = len(self._buffer)
        if available < 6:
            return None
        length = struct.unpack(">H", self._buffer[4:6])[0]
        marker_size = 6 + length
        if available < marker_size:
            return None
        marker_start = self._consumed
        raw = bytes(self._buffer[:marker_size])
        del self._buffer[:marker_size]
        self._consumed += marker_size
        try:
            trailer = json.loads(raw[6:].decode("utf-8"))
            counts = parse_counts(trailer)
        except (UnicodeDecodeError, ValueError, InputError) as exc:
            self.finished = True
            return StreamEvent(
                "bad_end_marker",
                marker_start,
                {"reason": str(exc), "bytes": marker_size},
            )
        self.finished = True
        self.counts = counts
        return StreamEvent(
            "end_marker", marker_start, {"counts": counts, "bytes": marker_size}
        )


def parse_stream(chunks):
    """Convenience helper: parse an iterable of byte chunks into events."""
    parser = FrameParser()
    for chunk in chunks:
        yield from parser.feed(chunk)
    yield from parser.finish()
