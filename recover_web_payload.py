"""Create a web archive from the last prepared FULL payload only.

This utility never imports the Claude client, never calls MT5 and never sends a
network request.  It is safe to use after a paid request whose response was
lost: Linux will receive the exact stored candles while the Claude fields stay
explicitly empty.
"""

import json
import sys
from pathlib import Path

from analysis_archive import save_payload_recovery_archive


BASE_DIR = Path(__file__).resolve().parent
PAYLOAD_PATH = BASE_DIR / "debug" / "claude_market_payload.json"


def main() -> int:
    if not PAYLOAD_PATH.exists():
        print(f"[RECOVERY ERROR] Не найден payload: {PAYLOAD_PATH}")
        return 2

    try:
        with open(PAYLOAD_PATH, "r", encoding="utf-8") as file:
            payload = json.load(file)
        archive_path = save_payload_recovery_archive(payload)
    except (OSError, json.JSONDecodeError, RuntimeError) as error:
        print(f"[RECOVERY ERROR] {type(error).__name__}: {error}")
        return 2

    print(f"[RECOVERY] Архив свечей создан: {archive_path}")
    print("[RECOVERY] Claude/MT5/network не вызывались.")
    print("[RECOVERY] Результат Claude отсутствует и повторно не запрашивался.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
