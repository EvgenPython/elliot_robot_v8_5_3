"""Compact post-run diagnostic summary for a saved runner console log."""

from __future__ import annotations

import argparse
import re
from collections import Counter, defaultdict
from pathlib import Path


def summarize(text: str) -> dict:
    dates = Counter(re.findall(r"2026-\d{2}-\d{2}", text))
    usage = defaultdict(lambda: {"calls": 0, "input": 0, "output": 0})
    pattern = re.compile(
        r"ANTHROPIC API — (SCOUT|FULL_MAP|FULL_DECISION).*?"
        r"(?:Input tokens:\s*([\d,]+).*?Output:\s*([\d,]+)|"
        r"\[TOKENS\] input=([\d,]+); output=([\d,]+))",
        re.S,
    )
    for match in pattern.finditer(text):
        stage = match.group(1)
        input_tokens = int((match.group(2) or match.group(4)).replace(",", ""))
        output_tokens = int((match.group(3) or match.group(5)).replace(",", ""))
        usage[stage]["calls"] += 1
        usage[stage]["input"] += input_tokens
        usage[stage]["output"] += output_tokens
    return {
        "dates_referenced": sorted(dates),
        "scout_decisions": text.count("SCOUT DECISION"),
        "scout_full_true": text.count("FULL required:   True"),
        "scout_full_false": text.count("FULL required:   False"),
        "full_archives": len(re.findall(r"FULL analysis сохранён", text)),
        "order_send_true": text.count("order_send called: True"),
        "api_failures": text.count("ATTEMPT FAILED"),
        "runner_errors": text.count("[RUNNER ERROR]"),
        "stale_heartbeats": text.count("Tick fresh:       False"),
        "usage": dict(usage),
    }


def main():
    parser = argparse.ArgumentParser(description="Сводка журнала Robot Elliot")
    parser.add_argument("log", type=Path)
    args = parser.parse_args()
    data = summarize(args.log.read_text(encoding="utf-8", errors="replace"))
    print("ROBOT ELLIOT — СВОДКА ЖУРНАЛА")
    print("Даты в данных:", ", ".join(data["dates_referenced"]) or "—")
    print(
        f"Scout: {data['scout_decisions']} | без FULL: {data['scout_full_false']} | "
        f"эскалаций: {data['scout_full_true']}"
    )
    print(f"FULL в архиве: {data['full_archives']}")
    print(f"order_send=True: {data['order_send_true']}")
    print(f"Ошибки API/runner: {data['api_failures']}/{data['runner_errors']}")
    print(f"Повторы stale heartbeat: {data['stale_heartbeats']}")
    for stage, item in data["usage"].items():
        print(f"{stage}: calls={item['calls']} input={item['input']:,} output={item['output']:,}")


if __name__ == "__main__":
    main()
