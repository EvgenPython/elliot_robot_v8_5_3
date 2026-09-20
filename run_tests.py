"""Offline release checks. Real MT5 and network access are disabled."""
import contextlib
import os
import re
import socket
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def main():
    root = Path(__file__).resolve().parent
    source = "\n".join(path.read_text(encoding="utf-8") for path in root.glob("*.py"))
    mt5 = types.ModuleType("MetaTrader5")
    for index, name in enumerate(sorted(set(re.findall(r"mt5\.([A-Z][A-Z0-9_]+)", source))), 1):
        setattr(mt5, name, index)
    mt5.ACCOUNT_TRADE_MODE_DEMO = 0
    mt5.ACCOUNT_TRADE_MODE_CONTEST = 1
    mt5.ACCOUNT_TRADE_MODE_REAL = 2
    def forbidden(*args, **kwargs):
        raise AssertionError("Offline tests: unmocked external operation is forbidden")
    for name in set(re.findall(r"mt5\.([a-z][a-z0-9_]+)", source)):
        setattr(mt5, name, forbidden)
    mt5.last_error = lambda: (0, "offline test")
    mt5.shutdown = lambda: None
    sys.modules["MetaTrader5"] = mt5
    # This fixture provides exception classes / a dummy client, not API access.
    import test_staged_analysis
    test_staged_analysis._install_runtime_stubs()
    with tempfile.TemporaryDirectory(prefix="robot_tests_") as temporary, \
         patch.dict(os.environ, {"ROBOT_DIAGNOSTICS_DIR": str(Path(temporary) / "events")}), \
         patch.object(socket.socket, "connect", forbidden), \
         patch.object(socket, "create_connection", forbidden):
        tests = unittest.defaultTestLoader.discover(str(root), pattern="test_*.py")
        result = unittest.TextTestRunner(verbosity=2).run(tests)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
