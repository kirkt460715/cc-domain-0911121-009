from dataclasses import dataclass
from decimal import Decimal
from ..errors import InputError
from ..observations import Issue, Measurement
from ..values import identifier, number, decimal_text, require_object, integer, digest


@dataclass(frozen=True)
class QualityPolicy:
    id: str
    unit: str
    lower: object = None
    upper: object = None
    maximum_uncertainty: object = None
    guard_band: bool = False
    warning_fraction: object = Decimal("0.05")
    required_tags: tuple = ()
    maximum_step: object = None
    maximum_gap_seconds: int = 0

    @classmethod
    def parse(cls, data, registry):
        fields = {
            "id",
            "unit",
            "lower",
            "upper",
            "maximum_uncertainty",
            "guard_band",
            "warning_fraction",
            "required_tags",
            "maximum_step",
            "maximum_gap_seconds",
        }
        require_object(data, fields, {"id", "unit"})
        registry.get(data["unit"])
        lower = (
            number(data["lower"], "lower") if data.get("lower") is not None else None
        )
        upper = (
            number(data["upper"], "upper") if data.get("upper") is not None else None
        )
        if lower is not None and upper is not None and lower >= upper:
            raise InputError("policy limits must increase")
        guard = data.get("guard_band", False)
        if not isinstance(guard, bool):
            raise InputError("guard_band must be boolean")
        fraction = number(
            data.get("warning_fraction", ".05"), "warning_fraction", nonnegative=True
        )
        if fraction >= Decimal(".5"):
            raise InputError("warning fraction must be below one half")
        tags = data.get("required_tags", [])
        if not isinstance(tags, list) or len(tags) > 20 or len(set(tags)) != len(tags):
            raise InputError("required_tags must be a unique bounded list")
        tags = tuple(identifier(tag, "required_tag") for tag in tags)
        uncertainty = data.get("maximum_uncertainty")
        step = data.get("maximum_step")
        return cls(
            identifier(data["id"]),
            data["unit"],
            lower,
            upper,
            number(uncertainty, nonnegative=True) if uncertainty is not None else None,
            guard,
            fraction,
            tags,
            number(step, positive=True) if step is not None else None,
            integer(
                data.get("maximum_gap_seconds", 0), "maximum_gap_seconds", 0, 31536000
            ),
        )

    def as_dict(self):
        result = {
            "id": self.id,
            "unit": self.unit,
            "guard_band": self.guard_band,
            "warning_fraction": decimal_text(self.warning_fraction),
            "required_tags": list(self.required_tags),
            "maximum_gap_seconds": self.maximum_gap_seconds,
        }
        for field in ("lower", "upper", "maximum_uncertainty", "maximum_step"):
            value = getattr(self, field)
            result[field] = decimal_text(value) if value is not None else None
        return result

    @property
    def fingerprint(self):
        return digest(self.as_dict())

    def evaluate(
        self,
        observation,
        value,
        uncertainty,
        unit,
        certificate,
        registry,
        previous=None,
    ):
        value = registry.convert(value, unit, self.unit)
        uncertainty = registry.uncertainty(uncertainty, unit, self.unit)
        issues = []
        low = value - uncertainty if self.guard_band else value
        high = value + uncertainty if self.guard_band else value
        if self.lower is not None and low < self.lower:
            issues.append(
                Issue(
                    "below_limit",
                    "reject",
                    "reading below lower quality limit",
                    {"checked": decimal_text(low), "limit": decimal_text(self.lower)},
                )
            )
        if self.upper is not None and high > self.upper:
            issues.append(
                Issue(
                    "above_limit",
                    "reject",
                    "reading above upper quality limit",
                    {"checked": decimal_text(high), "limit": decimal_text(self.upper)},
                )
            )
        if (
            self.maximum_uncertainty is not None
            and uncertainty > self.maximum_uncertainty
        ):
            issues.append(
                Issue(
                    "uncertainty",
                    "reject",
                    "combined uncertainty exceeds policy",
                    {"uncertainty": decimal_text(uncertainty)},
                )
            )
        for tag in self.required_tags:
            if tag not in observation.tags:
                issues.append(
                    Issue(
                        "missing_tag",
                        "reject",
                        "required provenance tag absent",
                        {"tag": tag},
                    )
                )
        if self.lower is not None and self.upper is not None:
            margin = (self.upper - self.lower) * self.warning_fraction
            if (
                self.lower <= low <= self.upper
                and min(low - self.lower, self.upper - high) < margin
            ):
                issues.append(
                    Issue("near_limit", "warning", "reading near a quality boundary")
                )
        if previous is not None:
            elapsed = (
                observation.event_time - previous.observation.event_time
            ).total_seconds()
            if elapsed <= 0:
                issues.append(
                    Issue(
                        "nonforward",
                        "warning",
                        "observation does not advance channel time",
                    )
                )
            else:
                if self.maximum_gap_seconds and elapsed > self.maximum_gap_seconds:
                    issues.append(
                        Issue(
                            "gap",
                            "warning",
                            "channel gap exceeds threshold",
                            {"seconds": elapsed},
                        )
                    )
                old = registry.convert(previous.value, previous.unit, self.unit)
                if (
                    self.maximum_step is not None
                    and abs(value - old) > self.maximum_step
                ):
                    issues.append(
                        Issue(
                            "step",
                            "reject",
                            "channel step exceeds threshold",
                            {"step": decimal_text(abs(value - old))},
                        )
                    )
        return Measurement(
            observation,
            value,
            uncertainty,
            self.unit,
            certificate,
            self.id,
            tuple(issues),
        )
