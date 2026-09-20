"""Central instrument configuration for current and future deployments."""
import json
import os
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config" / "instruments.json"
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9._-]{3,32}$")
DEFAULT_INSTRUMENT = "XAUUSD"


def _load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {"active_instrument": DEFAULT_INSTRUMENT, "instruments": []}
    value = json.loads(CONFIG_PATH.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise RuntimeError("config/instruments.json должен содержать JSON object.")
    return value


def active_instrument() -> str:
    raw = os.environ.get("ROBOT_INSTRUMENT") or _load_config().get(
        "active_instrument", DEFAULT_INSTRUMENT
    )
    symbol = str(raw).strip().upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise RuntimeError(f"Недопустимый ROBOT_INSTRUMENT: {symbol!r}.")
    return symbol


def instrument_catalog() -> list[dict]:
    config = _load_config()
    active = active_instrument()
    rows = config.get("instruments") if isinstance(config.get("instruments"), list) else []
    result, seen = [], set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        if not SYMBOL_PATTERN.fullmatch(symbol) or symbol in seen:
            continue
        seen.add(symbol)
        result.append({**row, "symbol": symbol, "enabled": symbol == active})
    if active not in seen:
        result.insert(0, {"symbol": active, "display_name": active, "enabled": True})
    return result


def symbol_state_path(filename: str) -> Path:
    """Preserve XAUUSD paths; isolate state for every later instrument."""
    symbol = active_instrument()
    state_dir = BASE_DIR / "state"
    return state_dir / filename if symbol == DEFAULT_INSTRUMENT else state_dir / symbol / filename

