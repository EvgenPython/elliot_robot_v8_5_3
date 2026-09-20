"""Показывает, почему за выбранные дни робот не дошёл до сделки."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path

from review_report import build_report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    days = max(1, min(90, int(args.days)))
    end = datetime.now().date()
    start = end - timedelta(days=days - 1)
    report = build_report(args.root, str(start), str(end))
    funnel = report["trade_funnel"]

    print("=" * 78)
    print(f"ВОРОНКА ТОРГОВЛИ ЗА {days} ДН.")
    print("=" * 78)
    print(f"Проверок с торговым решением: {funnel['evaluated_decisions']}")
    print(f"Решения Claude:               {funnel['claude_actions']}")
    print(f"Решения Risk Manager:         {funnel['risk_decisions']}")
    print(f"Результаты Executor:          {funnel['execution_decisions']}")
    print(f"Вызовов mt5.order_send:       {funnel['order_send_calls']}")
    if funnel["blockers"]:
        print("\nПричины отказа Risk Manager:")
        for reason, count in funnel["blockers"].items():
            print(f"  {count:>3} × {reason}")
    print("\nПоследние решения:")
    for row in funnel["latest"][-20:]:
        print(
            f"  {row.get('at')} | {row.get('cycle_type')} | "
            f"Claude={row.get('claude_action')} | Risk={row.get('risk_decision')} | "
            f"Executor={row.get('execution_decision')}"
        )
    if not funnel["latest"]:
        print("  В analysis_archive нет завершённых торговых решений за этот период.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
