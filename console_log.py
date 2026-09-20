"""UTF-8 console tee implemented inside Python (PowerShell 5 safe)."""

from __future__ import annotations

import atexit
import os
import sys
from pathlib import Path
from datetime import datetime, timezone


class _Tee:
    def __init__(self, console, log_file):
        self.console = console
        self.log_file = log_file

    def write(self, value):
        try:
            self.console.write(value)
        except (OSError, UnicodeError):
            pass
        try:
            self.log_file.write(value)
        except (OSError, ValueError):
            pass
        return len(value)

    def flush(self):
        for stream in (self.console, self.log_file):
            try:
                stream.flush()
            except (OSError, ValueError):
                pass

    def isatty(self):
        return bool(getattr(self.console, "isatty", lambda: False)())

    @property
    def encoding(self):
        return getattr(self.console, "encoding", "utf-8")


def install_console_log(role="runner") -> Path | None:
    if isinstance(sys.stdout, _Tee) and not sys.stdout.log_file.closed:
        return Path(sys.stdout.log_file.name)
    raw_path = os.getenv("ROBOT_LOG_FILE", "").strip()
    if not raw_path:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        raw_path = str(Path(__file__).resolve().parent / "logs" / f"{role}_{stamp}_{os.getpid()}.log")
    path = Path(raw_path).resolve()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        log_file = path.open("a", encoding="utf-8", buffering=1)
    except OSError:
        print("[LOG WARNING] Console file could not be opened; execution continues.", file=sys.stderr)
        return None
    sys.stdout = _Tee(sys.stdout, log_file)
    sys.stderr = _Tee(sys.stderr, log_file)
    atexit.register(log_file.close)
    return path
