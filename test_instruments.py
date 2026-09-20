import os
import unittest
from unittest.mock import patch

from instruments import DEFAULT_INSTRUMENT, active_instrument, instrument_catalog, symbol_state_path


class InstrumentConfigurationTests(unittest.TestCase):
    def test_default_release_enables_only_xauusd(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ROBOT_INSTRUMENT", None)
            self.assertEqual(active_instrument(), DEFAULT_INSTRUMENT)
            enabled = [item["symbol"] for item in instrument_catalog() if item["enabled"]]
            self.assertEqual(enabled, ["XAUUSD"])

    def test_future_symbol_uses_isolated_state_directory(self):
        with patch.dict(os.environ, {"ROBOT_INSTRUMENT": "EURUSD"}):
            path = symbol_state_path("analysis_state.json")
            self.assertEqual(path.parent.name, "EURUSD")


if __name__ == "__main__":
    unittest.main()
