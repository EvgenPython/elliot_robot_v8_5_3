import argparse
import json
from pathlib import Path

from claude_staged_client import (
    analyze_market_map,
    analyze_trade_decision,
    assemble_staged_analysis,
)
from claude_reference_state import get_fresh_reference_for_payload


BASE_DIR = Path(__file__).resolve().parent
DEBUG_PAYLOAD_PATH = BASE_DIR / "debug" / "claude_market_payload.json"


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Paid staged Claude analysis without trading execution."
    )
    parser.add_argument(
        "--confirm-paid-analysis",
        action="store_true",
        help="Explicitly allow the two paid Anthropic API stages.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)
    print("=" * 80)
    print("CLAUDE PROFESSIONAL TRADER — SAFE ANALYSIS TEST")
    print("=" * 80)
    print("[SAFE TEST] MT5 orders are NOT used by this script.")
    print("[SAFE TEST] Risk Manager and Trade State are NOT changed.")
    print("[INFO] This test makes TWO staged Anthropic API calls and consumes tokens.")
    print()

    if not args.confirm_paid_analysis:
        print(
            "[BLOCKED] Это платный тест. Для явного запуска добавьте "
            "--confirm-paid-analysis."
        )
        return 2

    if not DEBUG_PAYLOAD_PATH.exists():
        raise FileNotFoundError(
            "Не найден сохранённый Claude payload:\n"
            f"{DEBUG_PAYLOAD_PATH}"
        )

    with open(
        DEBUG_PAYLOAD_PATH,
        "r",
        encoding="utf-8",
    ) as file:
        payload = json.load(file)

    print(f"Payload:   {DEBUG_PAYLOAD_PATH}")
    print(f"Instrument:{' '}{payload.get('instrument')}")
    print(f"Snapshot:  {payload.get('timestamp')}")
    print()

    previous_reference = get_fresh_reference_for_payload(payload)

    print(
        "Previous reference: "
        f"{'YES (read-only)' if previous_reference else 'NO'}"
    )
    print()

    market_map = analyze_market_map(
        payload,
        previous_reference=previous_reference,
    )
    trade_decision = analyze_trade_decision(
        payload,
        market_map=market_map,
        previous_reference=previous_reference,
    )
    analysis = assemble_staged_analysis(
        payload=payload,
        market_map=market_map,
        trade_decision=trade_decision,
        previous_reference=previous_reference,
    )

    print()
    print("=" * 80)
    return 0
    print("[OK] SAFE PROFESSIONAL TRADER TEST COMPLETED")
    print("=" * 80)
    print(f"Action:        {analysis['recommendation']['action']}")
    print(f"Setup type:    {analysis['recommendation']['setup_type']}")
    print(f"Setup quality: {analysis['recommendation']['setup_quality']}")
    print(f"Market regime: {analysis['market_regime']['primary_regime']}")
    print(f"Current phase: {analysis['market_regime']['current_phase']}")
    print(f"Phase status:  {analysis['market_regime']['phase_status']}")
    print(f"Entry quality: {analysis['recommendation']['entry_quality']}")
    print("[SAFE TEST] Никакие торговые действия не выполнялись.")
    print("=" * 80)


if __name__ == "__main__":
    raise SystemExit(main())
