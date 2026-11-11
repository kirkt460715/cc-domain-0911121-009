from dataclasses import dataclass
from ..errors import InputError
from ..values import identifier, require_object, number, decimal_text, integer

FIELDS = {
    "id",
    "source",
    "sequence",
    "instrument",
    "channel",
    "event_time",
    "value",
    "unit",
    "uncertainty",
}
REQUIRED = FIELDS - {"uncertainty"}


@dataclass(frozen=True)
class Mapping:
    name: str
    columns: dict
    constants: dict
    tags: dict
    decimal_separator: str = "."

    @classmethod
    def parse(cls, data):
        require_object(
            data,
            {"name", "columns", "constants", "tags", "decimal_separator"},
            {"name", "columns"},
        )
        columns = require_object(data["columns"])
        constants = require_object(data.get("constants", {}))
        tags = require_object(data.get("tags", {}))
        if set(columns) - FIELDS or set(constants) - FIELDS:
            raise InputError("mapping targets unknown observation fields")
        if set(columns) & set(constants):
            raise InputError("a field cannot be both mapped and constant")
        if REQUIRED - (set(columns) | set(constants)):
            raise InputError("mapping omits required fields")
        if any(not isinstance(v, str) or not v.strip() for v in columns.values()):
            raise InputError("source column names must be nonempty strings")
        if any(not isinstance(v, str) or not v.strip() for v in tags.values()):
            raise InputError("tag mappings must identify source columns")
        for tag in tags:
            identifier(tag, "tag")
        separator = data.get("decimal_separator", ".")
        if separator not in {".", ","}:
            raise InputError("decimal separator must be dot or comma")
        return cls(
            identifier(data["name"]),
            dict(columns),
            dict(constants),
            dict(tags),
            separator,
        )

    def apply(self, record):
        require_object(record)
        result = dict(self.constants)
        missing = []
        for target, source in self.columns.items():
            if source not in record:
                missing.append(source)
            else:
                result[target] = record[source]
        if missing:
            raise InputError(
                "mapped columns absent", details={"columns": sorted(missing)}
            )
        for field in ("value", "uncertainty"):
            if field not in result:
                continue
            value = result[field]
            if isinstance(value, str) and self.decimal_separator == ",":
                if "." in value:
                    raise InputError("mixed decimal separators", field=field)
                value = value.replace(",", ".")
            result[field] = decimal_text(number(value, field))
        sequence = result.get("sequence")
        if isinstance(sequence, str):
            if not sequence.isascii() or not sequence.isdecimal():
                raise InputError("sequence must contain ASCII digits", field="sequence")
            sequence = int(sequence)
        result["sequence"] = integer(sequence, "sequence")
        result["tags"] = {}
        for tag, source in self.tags.items():
            if source not in record:
                raise InputError("missing tag column", field=source)
            result["tags"][tag] = record[source]
        return result

    def as_dict(self):
        return {
            "name": self.name,
            "columns": self.columns,
            "constants": self.constants,
            "tags": self.tags,
            "decimal_separator": self.decimal_separator,
        }

    def required_columns(self):
        return sorted(set(self.columns.values()) | set(self.tags.values()))

    def check_header(self, columns):
        if len(set(columns)) != len(columns):
            raise InputError("duplicate header names")
        missing = set(self.required_columns()) - set(columns)
        if missing:
            raise InputError(
                "header omits mapped columns", details={"columns": sorted(missing)}
            )
