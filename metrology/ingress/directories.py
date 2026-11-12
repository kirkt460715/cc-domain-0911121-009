"""Versioned gateway channel directories.

A directory document maps the numeric channels used by the binary protocol to
the instrument/channel/unit/source identifiers of the metrology catalog.  Each
directory version has a validity window that is selected against the frame
sampling time.  An unknown version is never approximated with the current
directory: frames carrying it stay deferred until the version is registered.
"""

from ..errors import InputError, MetrologyError, Missing
from ..values import (
    canonical,
    strict_json,
    identifier,
    integer,
    instant,
    time_text,
    digest,
)
from ..ledger.audit import append


class DirectoryDeferred(MetrologyError):
    code = "directory_deferred"


class ChannelDirectoryRegistry:
    def __init__(self, database):
        self.db = database
        with self.db.transaction() as con:
            con.execute(
                """CREATE TABLE IF NOT EXISTS gateway_directories(version INTEGER PRIMARY KEY,
                valid_from TEXT NOT NULL, valid_until TEXT NOT NULL,
                payload TEXT NOT NULL, fingerprint TEXT NOT NULL, registered_at TEXT NOT NULL)"""
            )

    @staticmethod
    def parse(data):
        from ..values import require_object

        require_object(
            data,
            {"version", "valid_from", "valid_until", "channels"},
            {"version", "valid_from", "valid_until"},
        )
        version = integer(data["version"], "version", 0, 65535)
        valid_from = instant(data["valid_from"], "valid_from")
        valid_until = instant(data["valid_until"], "valid_until")
        if valid_until <= valid_from:
            raise InputError("directory validity must be a nonempty half-open interval")
        channels = data.get("channels", {})
        if not isinstance(channels, dict) or not 1 <= len(channels) <= 64:
            raise InputError("directory must list one to sixty-four channels")
        normalized = {}
        for key, entry in channels.items():
            if not isinstance(key, str) or not key.isascii() or not key.isdecimal():
                raise InputError("directory channel key must be a decimal string")
            channel_number = integer(int(key), "channel", 0, 65535)
            if str(channel_number) != key:
                raise InputError("directory channel key must have no leading zeroes")
            if not isinstance(entry, dict):
                raise InputError("directory channel entry must be an object")
            required = {"instrument", "channel", "unit", "source"}
            if set(entry) != required:
                raise InputError(
                    "directory channel entry fields must be exactly instrument, channel, unit, source"
                )
            normalized[str(channel_number)] = {
                "instrument": identifier(entry["instrument"], "instrument"),
                "channel": identifier(entry["channel"], "channel"),
                "unit": identifier(entry["unit"], "unit"),
                "source": identifier(entry["source"], "source"),
            }
        document = {
            "version": version,
            "valid_from": time_text(valid_from),
            "valid_until": time_text(valid_until),
            "channels": normalized,
        }
        return document

    def register(self, data, *, registered_at=None):
        document = self.parse(data)
        fingerprint = digest(document)
        with self.db.transaction() as con:
            previous = con.execute(
                "SELECT payload FROM gateway_directories WHERE version=?",
                (document["version"],),
            ).fetchone()
            if previous is not None:
                if previous["payload"] != canonical(document):
                    from ..errors import Conflict

                    raise Conflict("directory version is immutable")
                return False
            con.execute(
                "INSERT INTO gateway_directories VALUES(?,?,?,?,?,?)",
                (
                    document["version"],
                    document["valid_from"],
                    document["valid_until"],
                    canonical(document),
                    fingerprint,
                    registered_at or time_text(instant("1970-01-01T00:00:00Z")),
                ),
            )
            append(
                con,
                "gateway_directory_registered",
                str(document["version"]),
                {"version": document["version"], "fingerprint": fingerprint},
            )
        return True

    def document(self, version):
        integer(version, "version", 0, 65535)
        row = self.db.one(
            "SELECT payload FROM gateway_directories WHERE version=?", (version,)
        )
        if row is None:
            raise Missing("channel directory version not registered", field=str(version))
        return strict_json(row["payload"])

    def listing(self):
        return [
            strict_json(row["payload"])
            for row in self.db.rows(
                "SELECT payload FROM gateway_directories ORDER BY version"
            )
        ]

    def resolve(self, version, channel_number, sampled_at):
        """Return the mapping for one frame, or raise DirectoryDeferred."""
        try:
            document = self.document(version)
        except Missing as exc:
            raise DirectoryDeferred(
                "channel directory version is not registered yet",
                field=str(version),
                details={"version": version},
            ) from exc
        key = str(integer(channel_number, "channel", 0, 65535))
        entry = document["channels"].get(key)
        if entry is None:
            raise InputError(
                "channel absent from directory version",
                field=key,
                details={"version": version},
            )
        sampled = instant(sampled_at) if isinstance(sampled_at, str) else sampled_at
        if not (
            instant(document["valid_from"])
            <= sampled
            < instant(document["valid_until"])
        ):
            raise InputError(
                "frame sampling time is outside directory validity",
                details={
                    "version": version,
                    "valid_from": document["valid_from"],
                    "valid_until": document["valid_until"],
                    "sampled_at": time_text(sampled),
                },
            )
        return entry
