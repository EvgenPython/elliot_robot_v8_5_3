"""Explicitly clear WaveFrame V8.5.3 permanent AI circuit breaker."""
from __future__ import annotations

import argparse

from claude_resilient_pipeline import clear_ai_circuit_breaker


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reason", required=True, help="What was fixed before reset.")
    parser.add_argument("--yes", action="store_true", help="Required safety acknowledgement.")
    args = parser.parse_args()
    if not args.yes:
        print("BLOCKED: add --yes after the permanent cause was actually fixed.")
        return 2
    result = clear_ai_circuit_breaker(reason=args.reason)
    print("AI circuit breaker cleared.")
    print(result.get("message_ru"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
