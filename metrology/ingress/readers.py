import csv
import io
from dataclasses import dataclass
from pathlib import Path
from ..errors import InputError, MetrologyError
from ..observations import Observation
from ..values import strict_json, require_object


@dataclass(frozen=True)
class InputRow:
    line: int
    observation: object = None
    error: object = None

    @property
    def valid(self):
        return self.error is None

    def as_dict(self):
        result = {"line": self.line, "valid": self.valid}
        if self.valid:
            result["observation"] = self.observation.as_dict()
        else:
            result["error"] = self.error.as_dict()
        return result


def load_text(path, maximum_bytes=16000000):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise InputError("input must be a regular nonsymlink file")
    if path.stat().st_size > maximum_bytes:
        raise InputError("input exceeds byte limit")
    raw = path.read_bytes()
    if len(raw) > maximum_bytes:
        raise InputError("input grew beyond byte limit")
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise InputError("input must be UTF-8") from exc


def parse_row(value, line, mapping):
    try:
        require_object(value)
        if mapping:
            value = mapping.apply(value)
        return InputRow(line, observation=Observation.parse(value))
    except MetrologyError as exc:
        return InputRow(line, error=exc)


def read_jsonl(text, mapping=None, *, maximum_rows=100000, maximum_line=65536):
    rows = []
    for line, raw in enumerate(text.splitlines(), 1):
        if not raw.strip():
            continue
        if len(rows) >= maximum_rows:
            raise InputError("input exceeds row limit")
        if len(raw.encode("utf-8")) > maximum_line:
            rows.append(InputRow(line, error=InputError("line exceeds byte limit")))
            continue
        try:
            value = strict_json(raw)
        except MetrologyError as exc:
            rows.append(InputRow(line, error=exc))
            continue
        rows.append(parse_row(value, line, mapping))
    return rows


def read_csv(text, mapping, *, delimiter=",", maximum_rows=100000):
    if mapping is None:
        raise InputError("CSV requires an explicit column mapping")
    if delimiter not in {",", ";", "\t", "|"}:
        raise InputError("unsupported delimiter")
    reader = csv.reader(io.StringIO(text, newline=""), delimiter=delimiter, strict=True)
    try:
        columns = next(reader)
    except StopIteration:
        raise InputError("CSV is empty") from None
    except csv.Error as exc:
        raise InputError("malformed CSV header") from exc
    mapping.check_header(columns)
    rows = []
    try:
        for values in reader:
            if not values:
                continue
            if len(rows) >= maximum_rows:
                raise InputError("input exceeds row limit")
            if len(values) != len(columns):
                rows.append(
                    InputRow(
                        reader.line_num,
                        error=InputError("CSV row width differs from header"),
                    )
                )
                continue
            rows.append(parse_row(dict(zip(columns, values)), reader.line_num, mapping))
    except csv.Error as exc:
        raise InputError(
            "malformed CSV record", details={"line": reader.line_num}
        ) from exc
    return rows


def read_input(text, format_name, mapping=None, **options):
    if format_name == "jsonl":
        return read_jsonl(text, mapping, **options)
    if format_name == "csv":
        return read_csv(text, mapping, **options)
    raise InputError("format must be csv or jsonl")


def summarize(rows):
    invalid = [row for row in rows if not row.valid]
    sources = {}
    for row in rows:
        if row.valid:
            source = row.observation.source
            sources[source] = sources.get(source, 0) + 1
    return {
        "total": len(rows),
        "valid": len(rows) - len(invalid),
        "invalid": len(invalid),
        "sources": sources,
        "errors": [row.as_dict() for row in invalid],
    }
