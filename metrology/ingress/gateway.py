"""Direct binary gateway collection into the metrology pipeline.

The collector owns everything the line-oriented readers cannot express:

* wrap-aware frame identity (boot_id plus an explicit wrap generation);
* retransmission versus same-sequence/different-payload conflicts;
* deferred frames whose directory version is not yet registered;
* bounded in-memory buffering and cancellable streaming sessions;
* stream-tail reconciliation against the declared per-channel frame counts;
* isolated reprocessing entries for deferred or individually supplied frames.
"""

import hashlib
import threading
from dataclasses import dataclass

from .binary import FrameParser, SEQUENCE_MODULUS
from .directories import DirectoryDeferred
from .transport import CollectionCancelled
from ..errors import InputError, Missing
from ..values import (
    canonical,
    strict_json,
    identifier,
    instant,
    time_text,
    decimal_text,
)
from ..observations import Observation
from ..ingress.readers import InputRow
from ..ledger.audit import append

MAX_PENDING_FRAMES = 10000


@dataclass(frozen=True)
class ReceivedFrame:
    frame: object
    receive_index: int
    digest: str
    generation: int
    boot_id: int
    channel: int


class GatewayCollector:
    def __init__(
        self,
        database,
        pipeline,
        registry,
        *,
        max_pending_frames=MAX_PENDING_FRAMES,
    ):
        self.db = database
        self.pipeline = pipeline
        self.registry = registry
        if not isinstance(max_pending_frames, int) or max_pending_frames < 1:
            raise InputError("pending frame bound must be positive")
        self.max_pending_frames = max_pending_frames
        with self.db.transaction() as con:
            con.execute(
                """CREATE TABLE IF NOT EXISTS gateway_sessions(id TEXT PRIMARY KEY,
                gateway TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT,
                state TEXT NOT NULL, summary TEXT NOT NULL)"""
            )
            con.execute(
                """CREATE TABLE IF NOT EXISTS gateway_frames(session TEXT NOT NULL,
                receive_index INTEGER NOT NULL, boot_id INTEGER NOT NULL,
                channel INTEGER NOT NULL, sequence INTEGER NOT NULL,
                generation INTEGER NOT NULL, digest TEXT NOT NULL,
                status TEXT NOT NULL, detail TEXT NOT NULL, raw BLOB NOT NULL,
                observation_id TEXT, PRIMARY KEY(session,receive_index))"""
            )
            con.execute(
                """CREATE TABLE IF NOT EXISTS gateway_events(session TEXT NOT NULL,
                position INTEGER NOT NULL, kind TEXT NOT NULL, offset INTEGER NOT NULL,
                evidence TEXT NOT NULL, PRIMARY KEY(session,position))"""
            )

    # ------------------------------------------------------------------ #
    # Session management
    # ------------------------------------------------------------------ #
    def open_session(self, session_id, gateway_id, *, received_at=None):
        identifier(session_id, "session")
        identifier(gateway_id, "gateway")
        started = (
            time_text(instant(received_at))
            if received_at
            else time_text(instant("1970-01-01T00:00:00Z"))
        )
        with self.db.transaction() as con:
            existing = con.execute(
                "SELECT id FROM gateway_sessions WHERE id=?", (session_id,)
            ).fetchone()
            if existing:
                from ..errors import Conflict

                raise Conflict("gateway session id already used")
            summary = self._empty_summary(gateway_id)
            con.execute(
                "INSERT INTO gateway_sessions VALUES(?,?,?,?,'open',?)",
                (session_id, gateway_id, started, None, canonical(summary)),
            )
            append(con, "gateway_session_opened", session_id, {"gateway": gateway_id})

    def collect(
        self,
        session_id,
        gateway_id,
        chunks,
        *,
        received_at=None,
        cancelled=None,
        parser=None,
    ):
        """Run one bounded, cancellable collection over an iterable of chunks."""
        self.open_session(
            session_id, gateway_id, received_at=received_at
        )
        timestamp = (
            time_text(instant(received_at))
            if received_at
            else time_text(instant("1970-01-01T00:00:00Z"))
        )
        parser = parser or FrameParser()
        state = _SessionState(self, session_id, gateway_id, parser, cancelled, timestamp)
        try:
            for chunk in chunks:
                state.ensure_not_cancelled()
                for event in parser.feed(chunk):
                    state.handle(event)
            for event in parser.finish():
                state.handle(event)
        except CollectionCancelled:
            return state.close("cancelled", append_batch=True)
        except Exception:
            try:
                state.close("failed", append_batch=False)
            except Exception:
                pass
            raise
        if parser.counts is None:
            state_name = (
                "trailer_corrupt"
                if state.summary["trailer"] == "corrupt"
                else "truncated"
            )
            return state.close(state_name, append_batch=True)
        return state.close("complete", append_batch=True)

    def collect_http(self, session_id, gateway_id, url, **options):
        from .transport import iter_chunks

        cancelled = options.pop("cancelled", None)
        chunk_size = options.pop("chunk_size", 512)
        timeout = options.pop("timeout", 10)
        received_at = options.pop("received_at", None)
        if options:
            raise InputError("unknown collection options", details=list(options))
        return self.collect(
            session_id,
            gateway_id,
            iter_chunks(
                url,
                chunk_size=chunk_size,
                timeout=timeout,
                cancelled=cancelled,
            ),
            received_at=received_at,
            cancelled=cancelled,
        )

    def status(self, session_id):
        identifier(session_id)
        row = self.db.one("SELECT * FROM gateway_sessions WHERE id=?", (session_id,))
        if row is None:
            raise Missing("gateway session not found")
        return {
            "id": session_id,
            "gateway": row["gateway"],
            "state": row["state"],
            "started_at": row["started_at"],
            "ended_at": row["ended_at"],
            "summary": strict_json(row["summary"]),
        }

    def sessions(self):
        return [
            self.status(row["id"])
            for row in self.db.rows("SELECT id FROM gateway_sessions ORDER BY id")
        ]

    def events(self, session_id):
        identifier(session_id)
        self.status(session_id)
        return [
            {
                "position": row["position"],
                "kind": row["kind"],
                "offset": row["offset"],
                "evidence": strict_json(row["evidence"]),
            }
            for row in self.db.rows(
                "SELECT * FROM gateway_events WHERE session=? ORDER BY position",
                (session_id,),
            )
        ]

    def frames(self, session_id, *, status=None):
        identifier(session_id)
        self.status(session_id)
        sql = "SELECT * FROM gateway_frames WHERE session=?"
        parameters = [session_id]
        if status is not None:
            sql += " AND status=?"
            parameters.append(status)
        sql += " ORDER BY receive_index"
        return [
            {
                "receive_index": row["receive_index"],
                "boot_id": row["boot_id"],
                "channel": row["channel"],
                "sequence": row["sequence"],
                "generation": row["generation"],
                "digest": row["digest"],
                "status": row["status"],
                "detail": strict_json(row["detail"]),
                "observation_id": row["observation_id"],
            }
            for row in self.db.rows(sql, tuple(parameters))
        ]

    # ------------------------------------------------------------------ #
    # Reprocessing entries
    # ------------------------------------------------------------------ #
    def reprocess_session(
        self, session_id, *, batch_id=None, received_at=None, attempt=None
    ):
        """Re-resolve deferred frames after their directory version arrives.

        A session can be reprocessed repeatedly as versions arrive; each call
        carrying new frames needs its own batch id, so a default attempt suffix
        is derived from the number of prior reprocessing batches.
        """
        status = self.status(session_id)
        deferred = self.frames(session_id, status="deferred")
        if not deferred:
            return {
                "session": session_id,
                "reprocessed": 0,
                "still_deferred": 0,
                "batch": None,
            }
        if batch_id is None:
            prior = self.db.one(
                "SELECT COUNT(*) AS n FROM audit WHERE kind=? AND object_id=?",
                ("gateway_frames_reprocessed", session_id),
            )["n"]
            batch_id = f"{session_id}-reprocess" if prior == 0 else (
                f"{session_id}-reprocess-{prior + 1}"
            )
        batch_id = identifier(batch_id, "batch")
        timestamp = time_text(instant(received_at)) if received_at else status["ended_at"]
        rows = []
        remaining = 0
        for item in deferred:
            raw = self.db.one(
                "SELECT raw FROM gateway_frames WHERE session=? AND receive_index=?",
                (session_id, item["receive_index"]),
            )["raw"]
            frame = self._frame_from_raw(raw)
            received = ReceivedFrame(
                frame,
                item["receive_index"],
                item["digest"],
                item["generation"],
                frame.payload.boot_id,
                frame.payload.channel,
            )
            row = self._build_row(
                status["gateway"], received, allow_defer=True
            )
            if row is None:
                remaining += 1
                continue
            rows.append((item["receive_index"], row))
        if not rows:
            return {
                "session": session_id,
                "reprocessed": 0,
                "still_deferred": remaining,
                "batch": None,
            }
        # Reception evidence positions must stay unique inside one pipeline batch.
        input_rows = [
            InputRow(position + 1, observation=row.observation)
            for position, (_, row) in enumerate(rows)
        ]
        result = self.pipeline.ingest(batch_id, input_rows, received_at=timestamp)
        with self.db.transaction() as con:
            for (original_index, row) in rows:
                con.execute(
                    """UPDATE gateway_frames SET status='ingested',
                    detail=?, observation_id=? WHERE session=? AND receive_index=?""",
                    (
                        canonical({"batch": batch_id}),
                        row.observation.id,
                        session_id,
                        original_index,
                    ),
                )
            append(
                con,
                "gateway_frames_reprocessed",
                session_id,
                {"batch": batch_id, "frames": len(rows), "still_deferred": remaining},
            )
        return {
            "session": session_id,
            "reprocessed": len(rows),
            "still_deferred": remaining,
            "batch": result,
        }

    def reprocess_frame(self, gateway_id, raw_frame, *, batch_id, received_at):
        """Isolated entry: turn one raw frame into a single-frame batch."""
        identifier(gateway_id, "gateway")
        identifier(batch_id, "batch")
        parser = FrameParser()
        events = parser.feed(bytes(raw_frame)) + parser.finish()
        frame_events = [event for event in events if event.kind == "frame"]
        if len(frame_events) != 1 or len(events) != 1:
            raise InputError(
                "isolated reprocessing requires exactly one complete frame",
                details={
                    "events": [
                        {"kind": event.kind, "evidence": _jsonable(event.evidence)}
                        for event in events
                    ]
                },
            )
        frame = frame_events[0].evidence["frame"]
        generation = 0
        received = ReceivedFrame(
            frame,
            1,
            hashlib.sha256(frame.raw).hexdigest(),
            generation,
            frame.payload.boot_id,
            frame.payload.channel,
        )
        row = self._build_row(gateway_id, received, allow_defer=False)
        return self.pipeline.ingest(batch_id, [row], received_at=received_at)

    # ------------------------------------------------------------------ #
    # Frame translation
    # ------------------------------------------------------------------ #
    @staticmethod
    def _frame_from_raw(raw):
        events = FrameParser().feed(bytes(raw))
        if not events or events[0].kind != "frame":
            raise InputError("stored gateway frame is not decodable")
        return events[0].evidence["frame"]

    def _source_id(self, gateway_id, boot_id):
        return f"{gateway_id}.boot{boot_id}"

    def _observation_id(self, gateway_id, received):
        payload = received.frame.payload
        return "gw-{}-b{}-c{}-g{}-s{}".format(
            gateway_id,
            payload.boot_id,
            payload.channel,
            received.generation,
            payload.sequence,
        )

    def _build_row(self, gateway_id, received, *, allow_defer):
        frame = received.frame
        payload = frame.payload
        try:
            mapping = self.registry.resolve(
                payload.directory_version, payload.channel, payload.sampled_at
            )
        except DirectoryDeferred:
            if not allow_defer:
                raise
            return None
        source_id = self._source_id(gateway_id, payload.boot_id)
        self._ensure_source(source_id)
        global_sequence = received.generation * SEQUENCE_MODULUS + payload.sequence
        observation = Observation.parse(
            {
                "id": self._observation_id(gateway_id, received),
                "source": source_id,
                "sequence": global_sequence,
                "instrument": mapping["instrument"],
                "channel": mapping["channel"],
                "event_time": time_text(payload.sampled_at),
                "value": decimal_text(payload.raw_value),
                "unit": mapping["unit"],
                "uncertainty": "0",
                "tags": {
                    "boot_id": str(payload.boot_id),
                    "gateway_sequence": str(payload.sequence),
                    "directory_version": str(payload.directory_version),
                    "frame_offset": str(frame.offset),
                },
            }
        )
        return InputRow(received.receive_index, observation=observation)

    def _ensure_source(self, source_id):
        try:
            self.pipeline.catalog.source(source_id)
        except Missing:
            self.pipeline.catalog.add_source(source_id)

    @staticmethod
    def _empty_summary(gateway_id):
        return {
            "gateway": gateway_id,
            "frames_seen": 0,
            "frames_received": 0,
            "retransmissions": 0,
            "conflicts": 0,
            "deferred": 0,
            "ingested": 0,
            "quarantined": 0,
            "declared_counts": {},
            "unique_frames": {},
            "missing": {},
            "unexpected": {},
            "trailer": None,
        }


class _SessionState:
    """Mutable per-session machinery; never reused across sessions."""

    def __init__(self, collector, session_id, gateway_id, parser, cancelled, timestamp):
        self.collector = collector
        self.session_id = session_id
        self.gateway_id = gateway_id
        self.parser = parser
        self.cancelled = cancelled
        self.timestamp = timestamp
        # key=(boot, channel, sequence) -> digest/generation/reception evidence
        self.seen = {}
        # key=(boot, channel) -> {"last_seq","generation"}
        self.tracks = {}
        self.rows = []
        self.receive_index = 0
        self.event_position = 0
        self.summary = collector._empty_summary(gateway_id)
        self.lock = threading.RLock()

    def ensure_not_cancelled(self):
        if self.cancelled is not None and self.cancelled():
            raise CollectionCancelled(self.session_id)

    def handle(self, event):
        with self.lock:
            if event.kind == "frame":
                self.summary["frames_seen"] += 1
                self._handle_frame(event.evidence["frame"])
                return
            if event.kind == "crc_failure":
                # A frame-shaped object whose content cannot be trusted still
                # counts as a reception attempt and drives gap reconciliation.
                self.summary["frames_seen"] += 1
            self._record_event(event.kind, event.offset, event.evidence)
            if event.kind == "end_marker":
                self.summary["trailer"] = "complete"
                self.summary["declared_counts"] = {
                    f"{boot}:{channel}": count
                    for (boot, channel), count in event.evidence["counts"].items()
                }
            elif event.kind == "bad_end_marker":
                self.summary["trailer"] = "corrupt"

    def _handle_frame(self, frame):
        self.ensure_not_cancelled()
        payload = frame.payload
        key = (payload.boot_id, payload.channel, payload.sequence)
        track = self.tracks.setdefault(
            (payload.boot_id, payload.channel),
            {"last_seq": None, "generation": 0},
        )
        digest = hashlib.sha256(frame.raw).hexdigest()
        index = self.receive_index
        self.receive_index += 1
        if key in self.seen:
            previous = self.seen[key]
            self.summary["frames_received"] += 1
            if previous["digest"] == digest:
                self.summary["retransmissions"] += 1
                self._store_frame(
                    frame,
                    previous["generation"],
                    digest,
                    "retransmitted",
                    {"original_index": previous["receive_index"]},
                    None,
                    index,
                )
                return
            self.summary["conflicts"] += 1
            self.rows.append(
                InputRow(
                    index,
                    error=InputError(
                        "same sequence carries a different payload",
                        details={
                            "boot_id": payload.boot_id,
                            "channel": payload.channel,
                            "sequence": payload.sequence,
                            "original_digest": previous["digest"],
                            "conflict_digest": digest,
                        },
                    ),
                )
            )
            self._store_frame(
                frame,
                previous["generation"],
                digest,
                "conflict",
                {"original_digest": previous["digest"]},
                None,
                index,
            )
            return
        if track["last_seq"] is not None and payload.sequence < track["last_seq"]:
            if track["last_seq"] == SEQUENCE_MODULUS - 1 and payload.sequence == 0:
                track["generation"] += 1
            else:
                self._record_event(
                    "out_of_order",
                    frame.offset,
                    {
                        "boot_id": payload.boot_id,
                        "channel": payload.channel,
                        "sequence": payload.sequence,
                        "previous_sequence": track["last_seq"],
                    },
                )
        generation = track["generation"]
        track["last_seq"] = payload.sequence
        self.seen[key] = {
            "digest": digest,
            "generation": generation,
            "receive_index": index,
        }
        self.summary["frames_received"] += 1
        unique = self.summary["unique_frames"]
        bucket = f"{payload.boot_id}:{payload.channel}"
        unique[bucket] = unique.get(bucket, 0) + 1
        received = ReceivedFrame(
            frame,
            index,
            digest,
            generation,
            payload.boot_id,
            payload.channel,
        )
        row = self.collector._build_row(self.gateway_id, received, allow_defer=True)
        if row is None:
            self.summary["deferred"] += 1
            self._store_frame(
                frame,
                generation,
                digest,
                "deferred",
                {"directory_version": payload.directory_version},
                None,
                index,
            )
            if self.summary["deferred"] > self.collector.max_pending_frames:
                raise InputError(
                    "deferred frame cache bound reached",
                    details={"bound": self.collector.max_pending_frames},
                )
            return
        self.rows.append(row)
        self._store_frame(frame, generation, digest, "accepted", {}, row.observation.id, index)

    def _store_frame(
        self, frame, generation, digest, status, detail, observation_id, index
    ):
        with self.collector.db.transaction() as con:
            con.execute(
                """INSERT INTO gateway_frames(session,receive_index,boot_id,channel,
                sequence,generation,digest,status,detail,raw,observation_id)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    self.session_id,
                    index,
                    frame.payload.boot_id,
                    frame.payload.channel,
                    frame.payload.sequence,
                    generation,
                    digest,
                    status,
                    canonical(detail),
                    frame.raw,
                    observation_id,
                ),
            )

    def _record_event(self, kind, offset, evidence):
        with self.collector.db.transaction() as con:
            con.execute(
                "INSERT INTO gateway_events(session,position,kind,offset,evidence) VALUES(?,?,?,?,?)",
                (
                    self.session_id,
                    self.event_position,
                    kind,
                    offset,
                    canonical(_jsonable(evidence)),
                ),
            )
        self.event_position += 1

    def close(self, state, *, append_batch):
        batch = None
        if append_batch and self.rows:
            batch = self.collector.pipeline.ingest(
                self.session_id, self.rows, received_at=self.timestamp
            )
            self.summary["ingested"] = batch["inserted"]
            self.summary["quarantined"] = batch["quarantined"]
        self._reconcile()
        with self.collector.db.transaction() as con:
            con.execute(
                "UPDATE gateway_sessions SET ended_at=?,state=?,summary=? WHERE id=?",
                (self.timestamp, state, canonical(self.summary), self.session_id),
            )
            append(
                con,
                "gateway_session_closed",
                self.session_id,
                {"state": state, "summary": self.summary},
            )
        status = self.collector.status(self.session_id)
        status["batch"] = batch
        return status

    def _reconcile(self):
        if self.parser.counts is None:
            self.summary["missing"] = {}
            self.summary["unexpected"] = {}
            return
        missing = {}
        unexpected = {}
        declared_buckets = {
            f"{boot}:{channel}": count
            for (boot, channel), count in self.parser.counts.items()
        }
        all_buckets = set(declared_buckets) | set(self.summary["unique_frames"])
        for bucket in sorted(all_buckets):
            declared = declared_buckets.get(bucket, 0)
            received_count = self.summary["unique_frames"].get(bucket, 0)
            if received_count < declared:
                missing[bucket] = {
                    "declared": declared,
                    "received": received_count,
                    "missing": declared - received_count,
                }
            elif received_count > declared:
                unexpected[bucket] = {
                    "declared": declared,
                    "received": received_count,
                    "extra": received_count - declared,
                }
        self.summary["missing"] = missing
        self.summary["unexpected"] = unexpected


def _jsonable(value):
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if isinstance(key, tuple):
                key = ":".join(str(part) for part in key)
            result[key] = _jsonable(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if hasattr(value, "as_dict"):
        return value.as_dict()
    return value
