from ..calibration.units import Unit, UnitRegistry
from ..calibration.certificates import Certificate, CertificateBook
from ..processing.policy import QualityPolicy
from ..errors import Conflict, Missing, InputError, StateError
from ..values import (
    canonical,
    strict_json,
    identifier,
    label,
    integer,
    require_object,
    time_text,
    instant,
)
from .audit import append


class Catalog:
    def __init__(self, database):
        self.db = database
        with self.db.transaction() as con:
            for unit in UnitRegistry.builtins():
                con.execute(
                    "INSERT OR IGNORE INTO units VALUES(?,?)",
                    (unit.symbol, canonical(unit.as_dict())),
                )

    def registry(self):
        return UnitRegistry(
            [
                Unit.parse(strict_json(row["payload"]))
                for row in self.db.rows("SELECT payload FROM units")
            ]
        )

    def add_unit(self, data):
        unit = Unit.parse(data)
        with self.db.transaction() as con:
            old = con.execute(
                "SELECT payload FROM units WHERE symbol=?", (unit.symbol,)
            ).fetchone()
            if old:
                if old["payload"] != canonical(unit.as_dict()):
                    raise Conflict("unit is immutable")
                return False
            con.execute(
                "INSERT INTO units VALUES(?,?)",
                (unit.symbol, canonical(unit.as_dict())),
            )
            append(con, "unit_added", unit.symbol, unit.as_dict())
        return True

    def add_instrument(self, data):
        require_object(data, {"id", "label", "channels"}, {"id", "label", "channels"})
        instrument_id = identifier(data["id"])
        channels = data["channels"]
        if not isinstance(channels, dict) or not channels or len(channels) > 64:
            raise InputError("instrument must have one to sixty-four channels")
        registry = self.registry()
        normalized = {}
        for channel, unit in channels.items():
            identifier(channel, "channel")
            registry.get(unit)
            normalized[channel] = unit
        payload = {
            "id": instrument_id,
            "label": label(data["label"]),
            "channels": normalized,
        }
        with self.db.transaction() as con:
            previous = con.execute(
                "SELECT payload FROM instruments WHERE id=?", (instrument_id,)
            ).fetchone()
            if previous:
                if previous["payload"] != canonical(payload):
                    raise Conflict("instrument definition is immutable")
                return False
            con.execute(
                "INSERT INTO instruments VALUES(?,?,1)",
                (instrument_id, canonical(payload)),
            )
            append(con, "instrument_added", instrument_id, payload)
        return True

    def instrument(self, instrument_id, active=True):
        row = self.db.one("SELECT * FROM instruments WHERE id=?", (instrument_id,))
        if row is None:
            raise Missing("instrument not registered", field=instrument_id)
        if active and not row["active"]:
            raise StateError("instrument retired", field=instrument_id)
        return strict_json(row["payload"])

    def set_instrument_active(self, instrument_id, active):
        if not isinstance(active, bool):
            raise InputError("active must be boolean")
        with self.db.transaction() as con:
            row = con.execute(
                "SELECT active FROM instruments WHERE id=?", (instrument_id,)
            ).fetchone()
            if row is None:
                raise Missing("instrument not registered")
            if bool(row["active"]) == active:
                return False
            con.execute(
                "UPDATE instruments SET active=? WHERE id=?",
                (int(active), instrument_id),
            )
            append(con, "instrument_state", instrument_id, {"active": active})
        return True

    def channel(self, instrument_id, channel):
        instrument = self.instrument(instrument_id)
        if channel not in instrument["channels"]:
            raise Missing("instrument channel not registered", field=channel)
        return instrument["channels"][channel]

    def certificates(self):
        rows = self.db.rows("SELECT * FROM certificates ORDER BY revoked DESC,id")
        registry = self.registry()
        revoked = [row["id"] for row in rows if row["revoked"]]
        book = CertificateBook(revoked=revoked)
        for row in rows:
            book.add(Certificate.parse(strict_json(row["payload"]), registry))
        return book

    def add_certificate(self, data):
        registry = self.registry()
        certificate = Certificate.parse(data, registry)
        with self.db.transaction() as con:
            channel_unit = self.channel(certificate.instrument, certificate.channel)
            registry.validate_pair(channel_unit, certificate.input_unit)
            old = con.execute(
                "SELECT fingerprint FROM certificates WHERE id=?", (certificate.id,)
            ).fetchone()
            if old:
                if old["fingerprint"] != certificate.fingerprint:
                    raise Conflict("certificate id content conflict")
                return False
            book = self.certificates()
            book.add(certificate)
            con.execute(
                "INSERT INTO certificates VALUES(?,?,?,?,?,?,?,0)",
                (
                    certificate.id,
                    certificate.instrument,
                    certificate.channel,
                    time_text(certificate.valid_from),
                    time_text(certificate.valid_to),
                    canonical(certificate.as_dict()),
                    certificate.fingerprint,
                ),
            )
            append(con, "certificate_added", certificate.id, certificate.as_dict())
        return True

    def revoke_certificate(self, certificate_id, reason):
        reason = label(reason, "reason")
        with self.db.transaction() as con:
            row = con.execute(
                "SELECT revoked FROM certificates WHERE id=?", (certificate_id,)
            ).fetchone()
            if row is None:
                raise Missing("certificate not found")
            if row["revoked"]:
                return False
            con.execute(
                "UPDATE certificates SET revoked=1 WHERE id=?", (certificate_id,)
            )
            append(con, "certificate_revoked", certificate_id, {"reason": reason})
        return True

    def add_policy(self, data):
        policy = QualityPolicy.parse(data, self.registry())
        with self.db.transaction() as con:
            old = con.execute(
                "SELECT fingerprint FROM policies WHERE id=?", (policy.id,)
            ).fetchone()
            if old:
                if old["fingerprint"] != policy.fingerprint:
                    raise Conflict("policy version cannot be changed")
                return False
            con.execute(
                "INSERT INTO policies VALUES(?,?,?)",
                (policy.id, canonical(policy.as_dict()), policy.fingerprint),
            )
            append(con, "policy_added", policy.id, policy.as_dict())
        return True

    def policy(self, policy_id):
        row = self.db.one("SELECT payload FROM policies WHERE id=?", (policy_id,))
        if row is None:
            raise Missing("quality policy not found")
        return QualityPolicy.parse(strict_json(row["payload"]), self.registry())

    def bind(self, instrument, channel, policy_id):
        with self.db.transaction() as con:
            unit = self.channel(instrument, channel)
            policy = self.policy(policy_id)
            self.registry().validate_pair(unit, policy.unit)
            old = con.execute(
                "SELECT policy FROM bindings WHERE instrument=? AND channel=?",
                (instrument, channel),
            ).fetchone()
            if old and old["policy"] == policy_id:
                return False
            con.execute(
                "INSERT INTO bindings VALUES(?,?,?) ON CONFLICT(instrument,channel) DO UPDATE SET policy=excluded.policy",
                (instrument, channel, policy_id),
            )
            append(
                con,
                "policy_bound",
                instrument,
                {"channel": channel, "policy": policy_id},
            )
        return True

    def bound_policy(self, instrument, channel):
        row = self.db.one(
            "SELECT policy FROM bindings WHERE instrument=? AND channel=?",
            (instrument, channel),
        )
        if row is None:
            raise Missing("channel has no quality policy")
        return self.policy(row["policy"])

    def add_source(self, source_id, allowed_lateness=0):
        identifier(source_id)
        integer(allowed_lateness, "allowed_lateness", 0, 31536000)
        with self.db.transaction() as con:
            row = con.execute(
                "SELECT allowed_lateness FROM sources WHERE id=?", (source_id,)
            ).fetchone()
            if row:
                if row["allowed_lateness"] != allowed_lateness:
                    raise Conflict("source lateness is immutable")
                return False
            maximum = con.execute(
                "SELECT MAX(sequence) AS n FROM observations WHERE source=?",
                (source_id,),
            ).fetchone()["n"]
            high = maximum if maximum is not None else -1
            con.execute(
                "INSERT INTO sources(id,allowed_lateness,high_sequence) VALUES(?,?,?)",
                (source_id, allowed_lateness, high),
            )
            append(
                con, "source_added", source_id, {"allowed_lateness": allowed_lateness}
            )
        return True

    def source(self, source_id):
        row = self.db.one("SELECT * FROM sources WHERE id=?", (source_id,))
        if row is None:
            raise Missing("source not registered")
        return row

    def watermark(self, source_id, value):
        timestamp = instant(value)
        with self.db.transaction() as con:
            row = self.source(source_id)
            if row["watermark"] is not None and timestamp < instant(row["watermark"]):
                raise Conflict("watermark cannot move backwards")
            formatted = time_text(timestamp)
            if formatted == row["watermark"]:
                return False
            con.execute(
                "UPDATE sources SET watermark=? WHERE id=?", (formatted, source_id)
            )
            append(con, "watermark", source_id, {"at": formatted})
        return True

    def inventory(self):
        return {
            "units": self.registry().definitions(),
            "instruments": [
                {**strict_json(row["payload"]), "active": bool(row["active"])}
                for row in self.db.rows("SELECT * FROM instruments ORDER BY id")
            ],
            "certificates": [
                {**strict_json(row["payload"]), "revoked": bool(row["revoked"])}
                for row in self.db.rows("SELECT * FROM certificates ORDER BY id")
            ],
            "policies": [
                strict_json(row["payload"])
                for row in self.db.rows("SELECT * FROM policies ORDER BY id")
            ],
            "bindings": self.db.rows(
                "SELECT * FROM bindings ORDER BY instrument,channel"
            ),
            "sources": self.db.rows("SELECT * FROM sources ORDER BY id"),
        }
