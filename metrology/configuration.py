from .errors import InputError
from .values import require_object, bounded_list, identifier, digest
from .ingress.mapping import Mapping
from .ledger.audit import append


class Configuration:
    def __init__(self, database, catalog):
        self.db = database
        self.catalog = catalog

    def apply(self, data):
        fields = {
            "id",
            "units",
            "instruments",
            "sources",
            "certificates",
            "policies",
            "bindings",
            "mappings",
        }
        require_object(data, fields, {"id"})
        config_id = identifier(data["id"])
        for field in fields - {"id"}:
            bounded_list(data.get(field, []), field, 10000)
        mappings = [Mapping.parse(item) for item in data.get("mappings", [])]
        if len({mapping.name for mapping in mappings}) != len(mappings):
            raise InputError("configuration mapping names must be unique")
        counts = dict.fromkeys(
            ("units", "instruments", "sources", "certificates", "policies", "bindings"),
            0,
        )
        with self.db.transaction() as con:
            for value in data.get("units", []):
                counts["units"] += self.catalog.add_unit(value)
            for value in data.get("instruments", []):
                counts["instruments"] += self.catalog.add_instrument(value)
            for value in data.get("sources", []):
                require_object(value, {"id", "allowed_lateness"}, {"id"})
                counts["sources"] += self.catalog.add_source(
                    value["id"], value.get("allowed_lateness", 0)
                )
            for value in data.get("certificates", []):
                counts["certificates"] += self.catalog.add_certificate(value)
            for value in data.get("policies", []):
                counts["policies"] += self.catalog.add_policy(value)
            for value in data.get("bindings", []):
                require_object(
                    value,
                    {"instrument", "channel", "policy"},
                    {"instrument", "channel", "policy"},
                )
                counts["bindings"] += self.catalog.bind(
                    value["instrument"], value["channel"], value["policy"]
                )
            append(
                con,
                "configuration_applied",
                config_id,
                {"fingerprint": digest(data), "inserted": counts},
            )
        return {
            "id": config_id,
            "inserted": counts,
            "mappings": [mapping.as_dict() for mapping in mappings],
        }

    def export(self, config_id):
        identifier(config_id)
        inventory = self.catalog.inventory()
        instruments = []
        for item in inventory["instruments"]:
            if item["active"]:
                instruments.append(
                    {key: value for key, value in item.items() if key != "active"}
                )
        certificates = []
        active_ids = {item["id"] for item in instruments}
        for item in inventory["certificates"]:
            if not item["revoked"] and item["instrument"] in active_ids:
                certificates.append(
                    {key: value for key, value in item.items() if key != "revoked"}
                )
        bindings = [
            item for item in inventory["bindings"] if item["instrument"] in active_ids
        ]
        sources = [
            {"id": item["id"], "allowed_lateness": item["allowed_lateness"]}
            for item in inventory["sources"]
            if item["active"]
        ]
        return {
            "id": config_id,
            "units": inventory["units"],
            "instruments": instruments,
            "sources": sources,
            "certificates": certificates,
            "policies": inventory["policies"],
            "bindings": bindings,
            "mappings": [],
        }

    def readiness(self, instrument, channel, start, end):
        from .values import instant

        problems = []
        try:
            channel_unit = self.catalog.channel(instrument, channel)
        except Exception as exc:
            from .errors import MetrologyError

            if not isinstance(exc, MetrologyError):
                raise
            return {"ready": False, "problems": [exc.as_dict()]}
        try:
            policy = self.catalog.bound_policy(instrument, channel)
            self.catalog.registry().validate_pair(channel_unit, policy.unit)
        except Exception as exc:
            from .errors import MetrologyError

            if not isinstance(exc, MetrologyError):
                raise
            problems.append(exc.as_dict())
        gaps = self.catalog.certificates().gaps(
            instrument, channel, instant(start), instant(end)
        )
        if gaps:
            problems.append({"code": "calibration_gaps", "intervals": gaps})
        if not any(source["active"] for source in self.catalog.inventory()["sources"]):
            problems.append({"code": "no_sources"})
        return {"ready": not problems, "problems": problems}
