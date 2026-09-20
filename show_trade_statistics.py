"""Print durable broker-confirmed statistics from state/trade_state.json."""

import json

from instruments import symbol_state_path
from trade_statistics import build_trade_statistics


def main() -> None:
    path = symbol_state_path("trade_state.json")
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        state = {}

    report = build_trade_statistics(state)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
