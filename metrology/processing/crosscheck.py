from decimal import Decimal, localcontext
from ..errors import InputError
from ..values import integer, number, decimal_text, time_text
from .diagnostics import Diagnostics


class PairedChannels:
    def __init__(self, reports, registry):
        self.diagnostics = Diagnostics(reports)
        self.registry = registry

    def compare(
        self,
        left_instrument,
        left_channel,
        right_instrument,
        right_channel,
        start,
        end,
        *,
        tolerance_seconds=1,
        maximum_difference="1",
        target_unit=None,
    ):
        integer(tolerance_seconds, "tolerance_seconds", 0, 86400)
        difference_limit = number(
            maximum_difference, "maximum_difference", nonnegative=True
        )
        left = [
            item
            for item in self.diagnostics.measurements(
                left_instrument, left_channel, start, end
            )
            if item.accepted
        ]
        right = [
            item
            for item in self.diagnostics.measurements(
                right_instrument, right_channel, start, end
            )
            if item.accepted
        ]
        if len(left) > 10000 or len(right) > 10000:
            raise InputError(
                "crosscheck supports at most ten thousand readings per channel"
            )
        unit = target_unit or (
            left[0].unit if left else right[0].unit if right else None
        )
        if unit is not None:
            self.registry.get(unit)
        unmatched_right = set(range(len(right)))
        pairs = []
        unmatched_left = []
        ambiguities = []
        for observation in left:
            candidates = []
            for index in unmatched_right:
                other = right[index]
                seconds = abs(
                    (
                        other.observation.event_time
                        - observation.observation.event_time
                    ).total_seconds()
                )
                if seconds <= tolerance_seconds:
                    candidates.append(
                        (
                            seconds,
                            other.observation.event_time,
                            other.observation.id,
                            index,
                        )
                    )
            candidates.sort()
            if not candidates:
                unmatched_left.append(observation.observation.id)
                continue
            closest = candidates[0][0]
            tied = [candidate for candidate in candidates if candidate[0] == closest]
            if len(tied) > 1:
                ambiguities.append(
                    {
                        "left": observation.observation.id,
                        "candidates": [candidate[2] for candidate in tied],
                    }
                )
                unmatched_left.append(observation.observation.id)
                continue
            index = candidates[0][3]
            other = right[index]
            unmatched_right.remove(index)
            left_value = self.registry.convert(
                observation.value, observation.unit, unit
            )
            right_value = self.registry.convert(other.value, other.unit, unit)
            left_uncertainty = self.registry.uncertainty(
                observation.uncertainty, observation.unit, unit
            )
            right_uncertainty = self.registry.uncertainty(
                other.uncertainty, other.unit, unit
            )
            with localcontext() as ctx:
                ctx.prec = 40
                difference = left_value - right_value
                uncertainty = (left_uncertainty**2 + right_uncertainty**2).sqrt()
                normalized = abs(difference) / uncertainty if uncertainty else None
            pairs.append(
                {
                    "left": observation.observation.id,
                    "right": other.observation.id,
                    "left_at": time_text(observation.observation.event_time),
                    "right_at": time_text(other.observation.event_time),
                    "time_difference_seconds": closest,
                    "difference": decimal_text(difference),
                    "combined_uncertainty": decimal_text(uncertainty),
                    "normalized_difference": decimal_text(normalized)
                    if normalized is not None
                    else None,
                    "within_limit": abs(difference) <= difference_limit,
                }
            )
        with localcontext() as ctx:
            ctx.prec = 40
            differences = [number(item["difference"]) for item in pairs]
            bias = (
                sum(differences, Decimal(0)) / len(differences) if differences else None
            )
            rmse = (
                (
                    sum((value * value for value in differences), Decimal(0))
                    / len(differences)
                ).sqrt()
                if differences
                else None
            )
        return {
            "left": {"instrument": left_instrument, "channel": left_channel},
            "right": {"instrument": right_instrument, "channel": right_channel},
            "unit": unit,
            "pairs": pairs,
            "unmatched_left": unmatched_left,
            "unmatched_right": [
                right[index].observation.id for index in sorted(unmatched_right)
            ],
            "ambiguities": ambiguities,
            "bias": decimal_text(bias) if bias is not None else None,
            "rmse": decimal_text(rmse) if rmse is not None else None,
            "passed": bool(pairs)
            and not ambiguities
            and not unmatched_left
            and not unmatched_right
            and all(item["within_limit"] for item in pairs),
        }
