import unittest

from metrology.errors import Conflict, InputError, Missing
from metrology.ingress.directories import (
    ChannelDirectoryRegistry,
    DirectoryDeferred,
)
from gateway_support import directory

from support import application


class DirectoryTests(unittest.TestCase):
    def setUp(self):
        self.app = application()
        self.addCleanup(self.app.close)
        self.registry = self.app.directories

    def test_register_is_idempotent_and_immutable(self):
        self.assertTrue(self.registry.register(directory(1)))
        self.assertFalse(self.registry.register(directory(1)))
        changed = directory(1)
        changed["valid_until"] = "2028-01-01T00:00:00Z"
        with self.assertRaises(Conflict):
            self.registry.register(changed)

    def test_document_validation(self):
        for field in ("version", "valid_from", "valid_until", "channels"):
            bad = directory(1)
            del bad[field]
            with self.subTest(field=field):
                with self.assertRaises(InputError):
                    self.registry.register(bad)
        with self.assertRaises(InputError):
            self.registry.register(directory(1, valid_until="2025-01-01T00:00:00Z"))
        bad_channel = directory(1)
        del bad_channel["channels"]["1"]["source"]
        with self.assertRaises(InputError):
            self.registry.register(bad_channel)

    def test_unknown_version_defers_instead_of_guessing(self):
        self.registry.register(directory(1))
        with self.assertRaises(DirectoryDeferred):
            self.registry.resolve(2, 1, "2026-11-11T00:00:00Z")
        with self.assertRaises(Missing):
            self.registry.document(2)

    def test_resolution_uses_sampling_time_window(self):
        self.registry.register(directory(1))
        mapping = self.registry.resolve(1, 1, "2026-11-11T00:00:00Z")
        self.assertEqual(mapping["instrument"], "probe1")
        # Half-open validity: the boundary itself is outside the window.
        with self.assertRaises(InputError):
            self.registry.resolve(1, 1, "2027-01-01T00:00:00Z")
        with self.assertRaises(InputError):
            self.registry.resolve(1, 9, "2026-11-11T00:00:00Z")

    def test_multiple_versions_select_independent_windows(self):
        old = directory(1, valid_until="2026-06-01T00:00:00Z")
        new = directory(2, valid_from="2026-06-01T00:00:00Z")
        self.registry.register(old)
        self.registry.register(new)
        self.assertEqual(
            self.registry.resolve(1, 1, "2026-05-01T00:00:00Z")["unit"], "degC"
        )
        self.assertEqual(
            self.registry.resolve(2, 1, "2026-07-01T00:00:00Z")["unit"], "degC"
        )
        self.assertEqual([item["version"] for item in self.registry.listing()], [1, 2])


if __name__ == "__main__":
    unittest.main()
