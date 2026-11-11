import unittest
from decimal import Decimal
from metrology.calibration.units import Unit, UnitRegistry
from metrology.calibration.certificates import Certificate, CertificateBook
from metrology.calibration.fitting import fit_linear, evaluate_fit, leave_one_out
from metrology.processing.policy import QualityPolicy
from metrology.errors import InputError, Conflict, CalibrationError
from metrology.values import (
    instant,
    number,
    strict_json,
    identifier,
    decimal_text,
    time_text,
)
from support import certificate, observation


class ValueBoundaryTests(unittest.TestCase):
    def test_decimal_rejects_nonfinite_and_boolean(self):
        for value in ("NaN", "Infinity", "-Infinity", True, {}, None):
            with self.subTest(value=value):
                with self.assertRaises(InputError):
                    number(value)
        self.assertEqual(number("0.01"), Decimal(".01"))
        self.assertEqual(decimal_text(Decimal("-0.0")), "0")
        self.assertEqual(decimal_text(Decimal("12.3400")), "12.34")

    def test_numbers_preserve_large_exact_values(self):
        self.assertEqual(number("9007199254740993"), Decimal("9007199254740993"))
        with self.assertRaises(InputError):
            number("1e25")
        with self.assertRaises(InputError):
            number("-1", nonnegative=True)
        with self.assertRaises(InputError):
            number("0", positive=True)
        self.assertEqual(number("0", nonnegative=True), 0)

    def test_timestamps_are_timezone_qualified(self):
        left = instant("2026-11-11T08:00:00+08:00")
        right = instant("2026-11-11T00:00:00Z")
        self.assertEqual(left, right)
        self.assertEqual(time_text(left), "2026-11-11T00:00:00.000000Z")
        with self.assertRaises(InputError):
            instant("2026-11-11T00:00:00")
        with self.assertRaises(InputError):
            instant("1969-01-01T00:00:00Z")
        with self.assertRaises(InputError):
            instant("not a timestamp")

    def test_json_duplicates_and_constants_fail(self):
        for text in ('{"a":1,"a":2}', '{"a":NaN}', "{broken}", '{"a":Infinity}'):
            with self.subTest(text=text):
                with self.assertRaises(InputError):
                    strict_json(text)
        self.assertEqual(strict_json('{"nested":{"a":1}}'), {"nested": {"a": 1}})
        self.assertEqual(strict_json("[1,2]"), [1, 2])

    def test_identifier_limits(self):
        self.assertEqual(identifier("a.b_c-1:2"), "a.b_c-1:2")
        for value in ("", "../x", "with space", "汉字", "a" * 129, None):
            with self.subTest(value=value):
                with self.assertRaises(InputError):
                    identifier(value)


class UnitTests(unittest.TestCase):
    def setUp(self):
        self.registry = UnitRegistry()

    def test_temperature_offsets_do_not_affect_uncertainty(self):
        self.assertEqual(self.registry.convert("0", "degC", "K"), Decimal("273.15"))
        self.assertEqual(self.registry.convert("273.15", "K", "degC"), 0)
        self.assertEqual(self.registry.uncertainty("2", "degC", "K"), 2)
        self.assertEqual(self.registry.convert("1", "bar", "kPa"), 100)
        self.assertEqual(self.registry.convert("1", "mm", "um"), 1000)

    def test_dimension_mismatch(self):
        with self.assertRaises(CalibrationError):
            self.registry.convert("1", "kg", "kPa")
        with self.assertRaises(CalibrationError):
            self.registry.get("unknown")
        self.assertFalse(self.registry.compatible("g", "V"))
        self.assertTrue(self.registry.compatible("g", "kg"))

    def test_custom_unit_is_immutable(self):
        value = {
            "symbol": "MPa",
            "dimension": "pressure",
            "scale": "1000000",
            "offset": "0",
        }
        first = self.registry.add(value)
        self.assertEqual(self.registry.add(value), first)
        self.assertEqual(self.registry.convert("1", "MPa", "kPa"), 1000)
        with self.assertRaises(Conflict):
            self.registry.add({**value, "scale": "1"})
        with self.assertRaises(InputError):
            Unit.parse({**value, "scale": "0"})
        symbols = [item["symbol"] for item in self.registry.definitions()]
        self.assertEqual(symbols, sorted(symbols))


class CertificateTests(unittest.TestCase):
    def setUp(self):
        self.registry = UnitRegistry()
        self.cert = Certificate.parse(certificate(), self.registry)

    def test_half_open_validity_and_ranges(self):
        book = CertificateBook([self.cert])
        selected = book.select("i1", "p", instant("2026-01-01T00:00:00Z"))
        self.assertEqual(selected.id, "c1")
        with self.assertRaises(CalibrationError):
            book.select("i1", "p", instant("2027-01-01T00:00:00Z"))
        self.assertEqual(self.cert.correct("100", "kPa", "0", self.registry)[0], 100)
        with self.assertRaises(CalibrationError):
            self.cert.correct("100.1", "kPa", "0", self.registry)
        with self.assertRaises(CalibrationError):
            self.cert.correct("-0.1", "kPa", "0", self.registry)

    def test_uncertainty_quadrature(self):
        cert = Certificate.parse(
            certificate(slope="2", intercept="1", uncertainty="4"), self.registry
        )
        value, uncertainty, unit = cert.correct("1000", "Pa", "1500", self.registry)
        self.assertEqual(value, 3)
        self.assertEqual(uncertainty, 5)
        self.assertEqual(unit, "kPa")
        self.assertEqual(Certificate.parse(cert.as_dict(), self.registry), cert)

    def test_overlap_conflict_and_revocation(self):
        book = CertificateBook([self.cert])
        self.assertFalse(book.add(self.cert))
        other = Certificate.parse(certificate(id="c2"), self.registry)
        with self.assertRaises(Conflict):
            book.add(other)
        self.assertTrue(book.revoke("c1"))
        self.assertFalse(book.revoke("c1"))
        self.assertTrue(book.add(other))
        self.assertEqual(
            book.select("i1", "p", instant("2026-06-01T00:00:00Z")).id, "c2"
        )
        with self.assertRaises(CalibrationError):
            book.revoke("missing")

    def test_adjacent_certificates_and_coverage(self):
        first = Certificate.parse(
            certificate(valid_to="2026-06-01T00:00:00Z"), self.registry
        )
        second = Certificate.parse(
            certificate(id="c2", valid_from="2026-06-01T00:00:00Z"), self.registry
        )
        book = CertificateBook([first, second])
        self.assertEqual(
            book.select("i1", "p", instant("2026-06-01T00:00:00Z")).id, "c2"
        )
        self.assertEqual(book.gaps("i1", "p", first.valid_from, second.valid_to), [])
        book.revoke("c2")
        gaps = book.gaps("i1", "p", first.valid_from, second.valid_to)
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]["start"], "2026-06-01T00:00:00.000000Z")
        with self.assertRaises(InputError):
            book.gaps("i1", "p", second.valid_to, first.valid_from)

    def test_invalid_certificate_rules(self):
        changes = [
            {"valid_to": "2025-01-01T00:00:00Z"},
            {"maximum": "0"},
            {"slope": "-1"},
            {"uncertainty": "-1"},
            {"output_unit": "kg"},
        ]
        for change in changes:
            with self.subTest(change=change):
                with self.assertRaises(InputError):
                    Certificate.parse(certificate(**change), self.registry)


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.registry = UnitRegistry()

    def test_guard_band_and_required_provenance(self):
        policy = QualityPolicy.parse(
            {
                "id": "q",
                "unit": "kPa",
                "lower": "0",
                "upper": "20",
                "guard_band": True,
                "required_tags": ["lot", "operator"],
            },
            self.registry,
        )
        measured = policy.evaluate(
            observation(value="19"), Decimal(19), Decimal(2), "kPa", "c1", self.registry
        )
        self.assertFalse(measured.accepted)
        self.assertIn("above_limit", [issue.code for issue in measured.issues])
        self.assertIn("missing_tag", [issue.code for issue in measured.issues])
        self.assertEqual(QualityPolicy.parse(policy.as_dict(), self.registry), policy)
        self.assertEqual(
            policy.fingerprint,
            QualityPolicy.parse(policy.as_dict(), self.registry).fingerprint,
        )

    def test_step_and_gap_rules(self):
        policy = QualityPolicy.parse(
            {"id": "q", "unit": "kPa", "maximum_step": "2", "maximum_gap_seconds": 5},
            self.registry,
        )
        previous = policy.evaluate(
            observation(0), Decimal(10), Decimal(0), "kPa", "c1", self.registry
        )
        current = policy.evaluate(
            observation(10),
            Decimal(15),
            Decimal(0),
            "kPa",
            "c1",
            self.registry,
            previous,
        )
        self.assertFalse(current.accepted)
        self.assertEqual({issue.code for issue in current.issues}, {"gap", "step"})
        same_time = policy.evaluate(
            observation(0),
            Decimal(10),
            Decimal(0),
            "kPa",
            "c1",
            self.registry,
            previous,
        )
        self.assertTrue(same_time.accepted)
        self.assertEqual(same_time.issues[0].code, "nonforward")

    def test_unit_conversion_before_comparison(self):
        policy = QualityPolicy.parse(
            {"id": "q", "unit": "Pa", "upper": "10001", "maximum_uncertainty": "10"},
            self.registry,
        )
        measured = policy.evaluate(
            observation(), Decimal(10), Decimal(".01"), "kPa", "c1", self.registry
        )
        self.assertTrue(measured.accepted)
        self.assertEqual(measured.value, 10000)
        self.assertEqual(measured.uncertainty, 10)
        self.assertEqual(measured.unit, "Pa")

    def test_bad_policy_rejected(self):
        changes = [
            {"lower": "2", "upper": "1"},
            {"warning_fraction": ".5"},
            {"guard_band": "yes"},
            {"required_tags": ["lot", "lot"]},
            {"maximum_step": "0"},
            {"maximum_gap_seconds": -1},
        ]
        for change in changes:
            with self.subTest(change=change):
                with self.assertRaises(InputError):
                    QualityPolicy.parse(
                        {"id": "q", "unit": "kPa", **change}, self.registry
                    )


class FittingTests(unittest.TestCase):
    def points(self):
        return [
            {
                "id": str(i),
                "indicated": str(i),
                "reference": str(2 * i + 1),
                "standard_uncertainty": "1",
            }
            for i in range(5)
        ]

    def test_exact_fit_and_prediction(self):
        fit = fit_linear(self.points(), residual_limit=".001")
        self.assertEqual(Decimal(fit["slope"]), 2)
        self.assertEqual(Decimal(fit["intercept"]), 1)
        self.assertEqual(Decimal(fit["maximum_residual"]), 0)
        self.assertTrue(fit["accepted"])
        self.assertEqual(evaluate_fit(fit, "2")["corrected"], "5")
        with self.assertRaises(CalibrationError):
            evaluate_fit(fit, "5")
        result = leave_one_out(self.points())
        self.assertEqual(len(result["leave_one_out"]), 5)
        self.assertTrue(
            all(
                Decimal(item["prediction_error"]) == 0
                for item in result["leave_one_out"]
            )
        )

    def test_residual_limit_reports_failure(self):
        points = self.points()
        points[2]["reference"] = "9"
        fit = fit_linear(points, residual_limit=".01")
        self.assertFalse(fit["accepted"])
        self.assertGreater(Decimal(fit["maximum_residual"]), 1)
        self.assertEqual(len(fit["points"]), 5)

    def test_singular_and_negative_slopes(self):
        points = self.points()
        for item in points:
            item["indicated"] = "1"
        with self.assertRaises(CalibrationError):
            fit_linear(points)
        points = self.points()
        for item in points:
            item["reference"] = str(-int(item["indicated"]))
        with self.assertRaises(CalibrationError):
            fit_linear(points)
        with self.assertRaises(InputError):
            fit_linear(self.points()[:2])
        points = self.points()
        points[1]["id"] = points[0]["id"]
        with self.assertRaises(InputError):
            fit_linear(points)
