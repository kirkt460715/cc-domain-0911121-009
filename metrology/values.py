import datetime as dt
import decimal
import hashlib
import json
import re
import unicodedata
from .errors import InputError

D = decimal.Decimal
UTC = dt.timezone.utc
IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


def identifier(value, field="id"):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise InputError("expected a bounded ASCII identifier", field=field)
    return value


def label(value, field="label", limit=512):
    if not isinstance(value, str):
        raise InputError("expected text", field=field)
    value = unicodedata.normalize("NFC", value).strip()
    if not value or len(value) > limit:
        raise InputError("empty or oversized text", field=field)
    if any(unicodedata.category(c) == "Cc" for c in value):
        raise InputError("control characters are not allowed", field=field)
    return value


def number(value, field="value", *, positive=False, nonnegative=False):
    if isinstance(value, bool) or not isinstance(value, (str, int, float, D)):
        raise InputError("expected a finite decimal", field=field)
    try:
        result = D(str(value))
    except decimal.InvalidOperation as exc:
        raise InputError("invalid decimal", field=field) from exc
    if not result.is_finite() or abs(result) > D("1e24"):
        raise InputError("decimal outside supported range", field=field)
    if result.as_tuple().exponent < -100:
        raise InputError("at most one hundred fractional places", field=field)
    if positive and result <= 0:
        raise InputError("expected a positive decimal", field=field)
    if nonnegative and result < 0:
        raise InputError("expected a nonnegative decimal", field=field)
    return result


def decimal_text(value):
    if value == 0:
        return "0"
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def integer(value, field, minimum=0, maximum=2**53 - 1):
    if isinstance(value, bool) or not isinstance(value, int):
        raise InputError("expected integer", field=field)
    if not minimum <= value <= maximum:
        raise InputError("integer outside supported range", field=field)
    return value


def instant(value, field="time"):
    if not isinstance(value, str):
        raise InputError("expected timezone-qualified ISO timestamp", field=field)
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InputError("invalid ISO timestamp", field=field) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InputError("timezone is required", field=field)
    parsed = parsed.astimezone(UTC)
    if parsed.year < 1970 or parsed.year > 2200:
        raise InputError("timestamp outside supported range", field=field)
    return parsed


def time_text(value):
    return (
        value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def epoch_microseconds(value):
    delta = value - dt.datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds


def from_microseconds(value):
    return dt.datetime(1970, 1, 1, tzinfo=UTC) + dt.timedelta(microseconds=value)


def canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def require_object(value, allowed=None, required=()):
    if not isinstance(value, dict):
        raise InputError("expected JSON object")
    missing = set(required) - value.keys()
    if missing:
        raise InputError("missing fields", details={"fields": sorted(missing)})
    if allowed is not None:
        unknown = value.keys() - set(allowed)
        if unknown:
            raise InputError("unknown fields", details={"fields": sorted(unknown)})
    return value


def strict_json(text):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise InputError("duplicate JSON key", field=key)
            result[key] = value
        return result

    def invalid(value):
        raise InputError("non-finite JSON number", details={"token": value})

    try:
        return json.loads(text, object_pairs_hook=pairs, parse_constant=invalid)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise InputError("malformed JSON", details={"reason": str(exc)}) from exc


def bounded_list(value, field, maximum=10000):
    if not isinstance(value, list) or len(value) > maximum:
        raise InputError("expected bounded list", field=field)
    return value


def bool_value(value, field):
    if not isinstance(value, bool):
        raise InputError("expected boolean", field=field)
    return value
