from dataclasses import dataclass
from decimal import Decimal, localcontext
from ..errors import CalibrationError, Conflict, InputError
from ..values import identifier, number, decimal_text, require_object


@dataclass(frozen=True)
class Unit:
    symbol: str
    dimension: str
    scale: Decimal
    offset: Decimal

    @classmethod
    def parse(cls, data):
        require_object(
            data,
            {"symbol", "dimension", "scale", "offset"},
            {"symbol", "dimension", "scale"},
        )
        return cls(
            identifier(data["symbol"], "symbol"),
            identifier(data["dimension"], "dimension"),
            number(data["scale"], "scale", positive=True),
            number(data.get("offset", 0), "offset"),
        )

    def as_dict(self):
        return {
            "symbol": self.symbol,
            "dimension": self.dimension,
            "scale": decimal_text(self.scale),
            "offset": decimal_text(self.offset),
        }


class UnitRegistry:
    def __init__(self, units=None):
        self._units = {}
        for unit in self.builtins() if units is None else units:
            self.add(unit)

    @staticmethod
    def builtins():
        return [
            Unit("K", "temperature", Decimal(1), Decimal(0)),
            Unit("degC", "temperature", Decimal(1), Decimal("273.15")),
            Unit("Pa", "pressure", Decimal(1), Decimal(0)),
            Unit("kPa", "pressure", Decimal(1000), Decimal(0)),
            Unit("bar", "pressure", Decimal(100000), Decimal(0)),
            Unit("m", "length", Decimal(1), Decimal(0)),
            Unit("mm", "length", Decimal(".001"), Decimal(0)),
            Unit("um", "length", Decimal(".000001"), Decimal(0)),
            Unit("kg", "mass", Decimal(1), Decimal(0)),
            Unit("g", "mass", Decimal(".001"), Decimal(0)),
            Unit("V", "voltage", Decimal(1), Decimal(0)),
            Unit("mV", "voltage", Decimal(".001"), Decimal(0)),
        ]

    def add(self, unit):
        if not isinstance(unit, Unit):
            unit = Unit.parse(unit)
        if unit.symbol in self._units and self._units[unit.symbol] != unit:
            raise Conflict("unit definition cannot be changed", field=unit.symbol)
        self._units[unit.symbol] = unit
        return unit

    def get(self, symbol):
        try:
            return self._units[symbol]
        except KeyError as exc:
            raise CalibrationError(
                "unknown unit", field="unit", details={"unit": symbol}
            ) from exc

    def compatible(self, source, target):
        return self.get(source).dimension == self.get(target).dimension

    def convert(self, value, source, target, *, delta=False):
        a, b = self.get(source), self.get(target)
        if a.dimension != b.dimension:
            raise CalibrationError(
                "incompatible unit dimensions",
                details={"source": source, "target": target},
            )
        value = number(value)
        with localcontext() as ctx:
            ctx.prec = 40
            base = value * a.scale
            if not delta:
                base += a.offset - b.offset
            return base / b.scale

    def uncertainty(self, value, source, target):
        value = number(value, "uncertainty", nonnegative=True)
        return self.convert(value, source, target, delta=True)

    def definitions(self):
        return [self._units[key].as_dict() for key in sorted(self._units)]

    def dimension(self, symbol):
        return self.get(symbol).dimension

    def validate_pair(self, source, target):
        if not self.compatible(source, target):
            raise InputError("source and target units differ in dimension")
