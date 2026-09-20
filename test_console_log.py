import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import console_log


class ConsoleLogTests(unittest.TestCase):
    def test_utf8_log_path_is_created(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "logs" / "runner.log"
            original_stdout, original_stderr = console_log.sys.stdout, console_log.sys.stderr
            try:
                with patch.dict(os.environ, {"ROBOT_LOG_FILE": str(path)}):
                    installed = console_log.install_console_log()
                    print("Русский журнал")
                    console_log.sys.stdout.flush()
                self.assertEqual(installed, path.resolve())
                self.assertIn("Русский журнал", path.read_text(encoding="utf-8"))
            finally:
                log_file = getattr(console_log.sys.stdout, "log_file", None)
                console_log.sys.stdout = original_stdout
                console_log.sys.stderr = original_stderr
                if log_file is not None and not log_file.closed:
                    log_file.close()


if __name__ == "__main__":
    unittest.main()
