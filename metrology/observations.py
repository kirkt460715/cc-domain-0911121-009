from dataclasses import dataclass, field
from .values import (
    identifier,
    integer,
    instant,
    number,
    require_object,
    time_text,
    decimal_text,
    digest,
    label,
)
from .errors import InputError


@dataclass(frozen=True)
class Observation:
    id: str
    source: str
    sequence: int
    instrument: str
    channel: str
    event_time: object
    value: object
    unit: str
    uncertainty: object
    tags: dict = field(default_factory=dict)

    @classmethod
    def parse(cls, value):
        fields = {
            "id",
            "source",
            "sequence",
            "instrument",
            "channel",
            "event_time",
            "value",
            "unit",
            "uncertainty",
            "tags",
        }
        require_object(value, fields, fields - {"tags", "uncertainty"})
        tags = value.get("tags", {})
        require_object(tags)
        if len(tags) > 20:
            raise InputError("at most twenty observation tags")
        tags = {
            identifier(k, "tag"): label(v, "tag value", 128) for k, v in tags.items()
        }
        return cls(
            identifier(value["id"]),
            identifier(value["source"], "source"),
            integer(value["sequence"], "sequence"),
            identifier(value["instrument"], "instrument"),
            identifier(value["channel"], "channel"),
            instant(value["event_time"]),
            number(value["value"]),
            identifier(value["unit"], "unit"),
            number(value.get("uncertainty", 0), "uncertainty", nonnegative=True),
            tags,
        )

    def as_dict(self):
        return {
            "id": self.id,
            "source": self.source,
            "sequence": self.sequence,
            "instrument": self.instrument,
            "channel": self.channel,
            "event_time": time_text(self.event_time),
            "value": decimal_text(self.value),
            "unit": self.unit,
            "uncertainty": decimal_text(self.uncertainty),
            "tags": dict(self.tags),
        }

    @property
    def fingerprint(self):
        return digest(self.as_dict())


@dataclass(frozen=True)
class Issue:
    code: str
    severity: str
    message: str
    evidence: dict = field(default_factory=dict)

    def as_dict(self):
        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "evidence": self.evidence,
        }


@dataclass(frozen=True)
class Measurement:
    observation: Observation
    value: object
    uncertainty: object
    unit: str
    certificate: str
    policy: str
    issues: tuple = ()

    @property
    def accepted(self):
        return not any(issue.severity == "reject" for issue in self.issues)

    def as_dict(self):
        return {
            "observation": self.observation.as_dict(),
            "corrected_value": decimal_text(self.value),
            "corrected_uncertainty": decimal_text(self.uncertainty),
            "unit": self.unit,
            "certificate": self.certificate,
            "policy": self.policy,
            "issues": [issue.as_dict() for issue in self.issues],
            "accepted": self.accepted,
        }

    @classmethod
    def restore(cls, value):
        return cls(
            Observation.parse(value["observation"]),
            number(value["corrected_value"]),
            number(value["corrected_uncertainty"]),
            value["unit"],
            value["certificate"],
            value["policy"],
            tuple(Issue(**issue) for issue in value["issues"]),
        )
