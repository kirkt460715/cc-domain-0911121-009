from dataclasses import dataclass
from decimal import localcontext
from ..errors import CalibrationError, Conflict, InputError
from ..values import (
    identifier,
    instant,
    time_text,
    number,
    decimal_text,
    require_object,
    digest,
)


@dataclass(frozen=True)
class Certificate:
    id: str
    instrument: str
    channel: str
    valid_from: object
    valid_to: object
    input_unit: str
    output_unit: str
    slope: object
    intercept: object
    uncertainty: object
    minimum: object
    maximum: object
    issued_at: object

    @classmethod
    def parse(cls, value, registry):
        fields = {
            "id",
            "instrument",
            "channel",
            "valid_from",
            "valid_to",
            "input_unit",
            "output_unit",
            "slope",
            "intercept",
            "uncertainty",
            "minimum",
            "maximum",
            "issued_at",
        }
        require_object(value, fields, fields)
        result = cls(
            identifier(value["id"]),
            identifier(value["instrument"], "instrument"),
            identifier(value["channel"], "channel"),
            instant(value["valid_from"]),
            instant(value["valid_to"]),
            identifier(value["input_unit"]),
            identifier(value["output_unit"]),
            number(value["slope"], "slope", positive=True),
            number(value["intercept"], "intercept"),
            number(value["uncertainty"], "uncertainty", nonnegative=True),
            number(value["minimum"]),
            number(value["maximum"]),
            instant(value["issued_at"]),
        )
        if result.valid_to <= result.valid_from:
            raise InputError(
                "certificate validity must be a nonempty half-open interval"
            )
        if result.maximum <= result.minimum:
            raise InputError("certificate range must be increasing")
        registry.validate_pair(result.input_unit, result.output_unit)
        return result

    def applies(self, instrument, channel, timestamp):
        return (
            self.instrument == instrument
            and self.channel == channel
            and self.valid_from <= timestamp < self.valid_to
        )

    def overlaps(self, other):
        return (
            self.instrument == other.instrument
            and self.channel == other.channel
            and max(self.valid_from, other.valid_from)
            < min(self.valid_to, other.valid_to)
        )

    def as_dict(self):
        return {
            "id": self.id,
            "instrument": self.instrument,
            "channel": self.channel,
            "valid_from": time_text(self.valid_from),
            "valid_to": time_text(self.valid_to),
            "input_unit": self.input_unit,
            "output_unit": self.output_unit,
            "slope": decimal_text(self.slope),
            "intercept": decimal_text(self.intercept),
            "uncertainty": decimal_text(self.uncertainty),
            "minimum": decimal_text(self.minimum),
            "maximum": decimal_text(self.maximum),
            "issued_at": time_text(self.issued_at),
        }

    @property
    def fingerprint(self):
        return digest(self.as_dict())

    def correct(self, raw_value, raw_unit, raw_uncertainty, registry):
        converted = registry.convert(raw_value, raw_unit, self.input_unit)
        if not self.minimum <= converted <= self.maximum:
            raise CalibrationError(
                "reading outside calibrated range", details={"certificate": self.id}
            )
        uncertainty = registry.uncertainty(raw_uncertainty, raw_unit, self.input_unit)
        with localcontext() as ctx:
            ctx.prec = 40
            corrected = converted * self.slope + self.intercept
            combined = ((uncertainty * self.slope) ** 2 + self.uncertainty**2).sqrt()
        return corrected, combined, self.output_unit


class CertificateBook:
    def __init__(self, certificates=(), revoked=()):
        self._certificates = {}
        self._revoked = set(revoked)
        for certificate in certificates:
            self.add(certificate)

    def add(self, certificate):
        previous = self._certificates.get(certificate.id)
        if previous is not None:
            if previous != certificate:
                raise Conflict("certificate id reused with different content")
            return False
        for other in self.active():
            if other.overlaps(certificate):
                raise Conflict(
                    "active certificate intervals overlap",
                    details={"existing": other.id},
                )
        self._certificates[certificate.id] = certificate
        return True

    def revoke(self, certificate_id):
        if certificate_id not in self._certificates:
            raise CalibrationError("unknown certificate")
        changed = certificate_id not in self._revoked
        self._revoked.add(certificate_id)
        return changed

    def select(self, instrument, channel, timestamp):
        candidates = [
            item
            for item in self.active()
            if item.applies(instrument, channel, timestamp)
        ]
        if not candidates:
            raise CalibrationError(
                "no valid calibration at observation time",
                details={
                    "instrument": instrument,
                    "channel": channel,
                    "at": time_text(timestamp),
                },
            )
        if len(candidates) != 1:
            raise CalibrationError("ambiguous calibration at observation time")
        return candidates[0]

    def active(self):
        return [
            item
            for key, item in sorted(self._certificates.items())
            if key not in self._revoked
        ]

    def gaps(self, instrument, channel, start, end):
        if end <= start:
            raise InputError("coverage interval is empty")
        intervals = sorted(
            (max(item.valid_from, start), min(item.valid_to, end))
            for item in self.active()
            if item.instrument == instrument
            and item.channel == channel
            and item.valid_to > start
            and item.valid_from < end
        )
        cursor = start
        result = []
        for left, right in intervals:
            if left > cursor:
                result.append((cursor, left))
            cursor = max(cursor, right)
        if cursor < end:
            result.append((cursor, end))
        return [
            {"start": time_text(left), "end": time_text(right)}
            for left, right in result
        ]
