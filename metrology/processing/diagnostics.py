from decimal import Decimal, localcontext
from collections import Counter
from ..errors import InputError
from ..values import number, decimal_text, integer, instant, time_text
from ..observations import Measurement


def percentile(values, probability):
    probability = number(probability, "probability", nonnegative=True)
    if probability > 1:
        raise InputError("percentile probability cannot exceed one")
    if not values:
        return None
    values = sorted(number(value) for value in values)
    with localcontext() as ctx:
        ctx.prec = 40
        position = probability * (len(values) - 1)
        low = int(position)
        high = min(low + 1, len(values) - 1)
        fraction = position - low
        return values[low] + fraction * (values[high] - values[low])


def robust_summary(values):
    values = [number(value) for value in values]
    if not values:
        return {
            "count": 0,
            "median": None,
            "mad": None,
            "q1": None,
            "q3": None,
            "outliers": [],
        }
    median = percentile(values, ".5")
    deviations = [abs(value - median) for value in values]
    mad = percentile(deviations, ".5")
    q1, q3 = percentile(values, ".25"), percentile(values, ".75")
    spread = q3 - q1
    low, high = q1 - Decimal("1.5") * spread, q3 + Decimal("1.5") * spread
    return {
        "count": len(values),
        "median": decimal_text(median),
        "mad": decimal_text(mad),
        "q1": decimal_text(q1),
        "q3": decimal_text(q3),
        "outliers": [
            index for index, value in enumerate(values) if value < low or value > high
        ],
    }


def stable_runs(measurements, minimum_run=3):
    integer(minimum_run, "minimum_run", 2, 10000)
    result = []
    current = []
    for measurement in measurements:
        if current and (
            measurement.unit != current[-1].unit
            or measurement.value != current[-1].value
        ):
            if len(current) >= minimum_run:
                result.append(current)
            current = []
        current.append(measurement)
    if len(current) >= minimum_run:
        result.append(current)
    return [
        {
            "first": run[0].observation.id,
            "last": run[-1].observation.id,
            "count": len(run),
            "value": decimal_text(run[0].value),
            "unit": run[0].unit,
            "duration_seconds": (
                run[-1].observation.event_time - run[0].observation.event_time
            ).total_seconds(),
        }
        for run in result
    ]


def channel_gaps(measurements, maximum_seconds):
    integer(maximum_seconds, "maximum_seconds", 1, 31536000)
    ordered = sorted(
        measurements,
        key=lambda item: (item.observation.event_time, item.observation.id),
    )
    gaps = []
    for left, right in zip(ordered, ordered[1:]):
        delta = (
            right.observation.event_time - left.observation.event_time
        ).total_seconds()
        if delta > maximum_seconds:
            gaps.append(
                {
                    "after": left.observation.id,
                    "before": right.observation.id,
                    "seconds": delta,
                }
            )
    return gaps


def ewma(measurements, alpha=".2", threshold=None):
    alpha = number(alpha, "alpha", positive=True)
    if alpha > 1:
        raise InputError("alpha cannot exceed one")
    threshold = number(threshold, positive=True) if threshold is not None else None
    output = []
    state = None
    unit = None
    with localcontext() as ctx:
        ctx.prec = 40
        for measurement in measurements:
            if unit is not None and measurement.unit != unit:
                raise InputError("EWMA requires a single unit")
            unit = measurement.unit
            old = state
            state = (
                measurement.value
                if state is None
                else alpha * measurement.value + (1 - alpha) * state
            )
            innovation = Decimal(0) if old is None else measurement.value - old
            output.append(
                {
                    "id": measurement.observation.id,
                    "ewma": decimal_text(state),
                    "innovation": decimal_text(innovation),
                    "alert": threshold is not None and abs(innovation) > threshold,
                }
            )
    return output


class Diagnostics:
    def __init__(self, reports):
        self.reports = reports

    def measurements(self, instrument, channel, start, end):
        cursor = None
        result = []
        while True:
            page = self.reports.query(
                instrument=instrument,
                channel=channel,
                start=start,
                end=end,
                after=cursor,
                limit=10000,
            )
            result.extend(
                Measurement.restore(item["measurement"])
                for item in page["items"]
                if item["measurement"]
            )
            cursor = page["next"]
            if cursor is None:
                return result

    def profile(
        self, instrument, channel, start, end, *, gap_seconds=60, minimum_run=3
    ):
        measurements = self.measurements(instrument, channel, start, end)
        groups = {}
        for measurement in measurements:
            groups.setdefault(measurement.unit, []).append(measurement)
        profiles = {}
        for unit, items in sorted(groups.items()):
            summary = robust_summary([item.value for item in items])
            summary["outliers"] = [
                items[index].observation.id for index in summary["outliers"]
            ]
            profiles[unit] = summary
        return {
            "instrument": instrument,
            "channel": channel,
            "start": time_text(instant(start)),
            "end": time_text(instant(end)),
            "profiles": profiles,
            "stable_runs": stable_runs(measurements, minimum_run),
            "gaps": channel_gaps(measurements, gap_seconds),
            "certificate_counts": dict(
                Counter(item.certificate for item in measurements)
            ),
            "policy_counts": dict(Counter(item.policy for item in measurements)),
        }

    def trend(self, instrument, channel, start, end, *, alpha=".2", threshold=None):
        measurements = self.measurements(instrument, channel, start, end)
        accepted = [measurement for measurement in measurements if measurement.accepted]
        return {
            "instrument": instrument,
            "channel": channel,
            "accepted": len(accepted),
            "points": ewma(accepted, alpha, threshold),
        }
