from dataclasses import dataclass
from decimal import Decimal, localcontext
from ..errors import InputError, CalibrationError
from ..values import number, identifier, decimal_text, require_object, digest


@dataclass(frozen=True)
class ReferencePair:
    id: str
    indicated: Decimal
    reference: Decimal
    standard_uncertainty: Decimal

    @classmethod
    def parse(cls, data):
        require_object(
            data,
            {"id", "indicated", "reference", "standard_uncertainty"},
            {"id", "indicated", "reference", "standard_uncertainty"},
        )
        return cls(
            identifier(data["id"]),
            number(data["indicated"]),
            number(data["reference"]),
            number(data["standard_uncertainty"], positive=True),
        )

    def as_dict(self):
        return {
            "id": self.id,
            "indicated": decimal_text(self.indicated),
            "reference": decimal_text(self.reference),
            "standard_uncertainty": decimal_text(self.standard_uncertainty),
        }


def fit_linear(data, *, residual_limit=None):
    if not isinstance(data, list) or not 3 <= len(data) <= 10000:
        raise InputError(
            "calibration fit requires three to ten thousand reference pairs"
        )
    pairs = [ReferencePair.parse(item) for item in data]
    if len({pair.id for pair in pairs}) != len(pairs):
        raise InputError("reference pair ids must be unique")
    if len({pair.indicated for pair in pairs}) < 2:
        raise CalibrationError("fit requires distinct indicated readings")
    limit = (
        number(residual_limit, positive=True) if residual_limit is not None else None
    )
    with localcontext() as ctx:
        ctx.prec = 40
        weights = [Decimal(1) / (pair.standard_uncertainty**2) for pair in pairs]
        total_weight = sum(weights)
        x_mean = sum(w * p.indicated for w, p in zip(weights, pairs)) / total_weight
        y_mean = sum(w * p.reference for w, p in zip(weights, pairs)) / total_weight
        xx = sum(w * (p.indicated - x_mean) ** 2 for w, p in zip(weights, pairs))
        xy = sum(
            w * (p.indicated - x_mean) * (p.reference - y_mean)
            for w, p in zip(weights, pairs)
        )
        if xx == 0:
            raise CalibrationError("singular calibration fit")
        slope = xy / xx
        if slope <= 0:
            raise CalibrationError("calibration fit must be monotonic increasing")
        intercept = y_mean - slope * x_mean
        residuals = [
            pair.reference - (slope * pair.indicated + intercept) for pair in pairs
        ]
        weighted_sse = sum(w * r * r for w, r in zip(weights, residuals))
        residual_scale = (weighted_sse / Decimal(len(pairs) - 2)).sqrt()
        slope_uncertainty = (Decimal(1) / xx).sqrt()
        intercept_uncertainty = (
            Decimal(1) / total_weight + x_mean * x_mean / xx
        ).sqrt()
        maximum_residual = max(abs(value) for value in residuals)
        result = {
            "slope": decimal_text(slope),
            "intercept": decimal_text(intercept),
            "slope_uncertainty": decimal_text(slope_uncertainty),
            "intercept_uncertainty": decimal_text(intercept_uncertainty),
            "weighted_residual_scale": decimal_text(residual_scale),
            "maximum_residual": decimal_text(maximum_residual),
            "minimum": decimal_text(min(pair.indicated for pair in pairs)),
            "maximum": decimal_text(max(pair.indicated for pair in pairs)),
            "accepted": limit is None or maximum_residual <= limit,
            "points": [
                {
                    "id": pair.id,
                    "residual": decimal_text(residual),
                    "standardized_residual": decimal_text(
                        residual / pair.standard_uncertainty
                    ),
                }
                for pair, residual in zip(pairs, residuals)
            ],
            "input_fingerprint": digest([pair.as_dict() for pair in pairs]),
        }
    return result


def evaluate_fit(fit, indicated):
    value = number(indicated)
    low = number(fit["minimum"])
    high = number(fit["maximum"])
    if value < low or value > high:
        raise CalibrationError("fit extrapolation is not supported")
    with localcontext() as ctx:
        ctx.prec = 40
        corrected = value * number(fit["slope"]) + number(fit["intercept"])
    return {
        "indicated": decimal_text(value),
        "corrected": decimal_text(corrected),
        "fit_fingerprint": fit["input_fingerprint"],
    }


def leave_one_out(data):
    if not isinstance(data, list) or len(data) < 4 or len(data) > 128:
        raise InputError(
            "leave-one-out requires four to one hundred twenty-eight pairs"
        )
    complete = fit_linear(data)
    diagnostics = []
    for index, raw in enumerate(data):
        pair = ReferencePair.parse(raw)
        subset = data[:index] + data[index + 1 :]
        try:
            fitted = fit_linear(subset)
        except CalibrationError as exc:
            diagnostics.append({"id": pair.id, "error": exc.as_dict()})
            continue
        with localcontext() as ctx:
            ctx.prec = 40
            predicted = number(fitted["slope"]) * pair.indicated + number(
                fitted["intercept"]
            )
            diagnostics.append(
                {
                    "id": pair.id,
                    "prediction_error": decimal_text(pair.reference - predicted),
                    "slope_change": decimal_text(
                        number(fitted["slope"]) - number(complete["slope"])
                    ),
                }
            )
    return {"fit": complete, "leave_one_out": diagnostics}
