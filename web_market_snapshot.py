"""Best-effort market snapshot for the read-only Linux web terminal.

The helper reuses the already collected MT5 snapshot.  It never connects to
MT5, never calls Claude and never changes trading state.  Any export failure
is deliberately contained so that monitoring cannot block the robot.
"""

import json
import os
from pathlib import Path

from claude_payload import build_claude_payload
from instruments import symbol_state_path


BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
WEB_MARKET_SNAPSHOT_PATH = symbol_state_path("web_market_snapshot.json")


def _atomic_write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")

    with open(temporary_path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, separators=(",", ":"))
        file.flush()
        os.fsync(file.fileno())

    os.replace(temporary_path, path)


def save_web_market_snapshot(
    snapshot: dict,
    path: Path = WEB_MARKET_SNAPSHOT_PATH,
) -> bool:
    """Stores exact raw market data for the web publisher, best effort only."""
    try:
        payload = build_claude_payload(snapshot)
        _atomic_write_json(path, payload)
        print(
            "[WEB SNAPSHOT] Свечи сохранены независимо от ответа Claude: "
            f"{path}"
        )
        return True
    except Exception as error:
        print(
            "[WEB SNAPSHOT WARNING] Не удалось сохранить свечи для веба; "
            "торговый цикл продолжается: "
            f"{type(error).__name__}: {error}"
        )
        return False
