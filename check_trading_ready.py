"""Безопасная проверка готовности торговли без формирования и отправки ордера."""

from execution_control import inspect_execution_safety_gate, print_execution_safety_gate
from mt5_client import connect_mt5, disconnect_mt5
from position_monitor import load_position_monitor_config
from runtime_policy import inspect_market_runtime_gate
from main import inspect_pre_claude_daily_state_gate
from trade_state import get_active_plan, get_managed_positions
from analysis_state import load_analysis_state
from observability import emit, VERSION


def main() -> int:
    print(f"ROBOT {VERSION}: ПРОВЕРКА ГОТОВНОСТИ — ОРДЕРА НЕ ОТПРАВЛЯЮТСЯ")
    if not connect_mt5():
        print("[BLOCKED] Не удалось подключиться к MT5.")
        return 1
    try:
        report = inspect_execution_safety_gate()
        print_execution_safety_gate(report)
        monitor = load_position_monitor_config()
        print()
        print("POSITION MONITOR / PROTECTION")
        print("-" * 70)
        print(f"Enabled:              {monitor['enabled']}")
        print(f"M15 Scout:            {monitor['m15_scout_enabled']}")
        print(f"Deep H1 review:       {monitor['deep_review_on_every_closed_h1']}")
        print(f"Automatic SL/TP:      {monitor['automatic_trade_changes']}")
        print(f"Minimum confidence:   {monitor['minimum_confidence']}")
        print("Stop rule:            wave 3 -> wave 1; wave 5 -> wave 3")
        print("Stop risk rule:       NEVER WIDEN")
        market = inspect_market_runtime_gate()
        daily = inspect_pre_claude_daily_state_gate()
        plan = get_active_plan()
        positions = get_managed_positions()
        print("\nCURRENT ENTRY CONDITIONS")
        print(f"Market/session gate:  {market.get('allowed')}")
        for reason in market.get("reasons", []):
            print(f"  - {reason}")
        print(f"Fresh tick:           {market.get('tick_fresh')}")
        print(f"Trusted daily base:   {daily.get('trusted')} ({daily.get('capture_method')})")
        print(f"Active entry plan:    {plan.get('execution_status') if plan else 'none'}")
        print(f"Managed positions:    {len(positions)}")
        print(f"Last analyzed H1:     {load_analysis_state().get('last_analyzed_h1')}")
        emit("readiness", "report", data={"execution": report, "market": market, "daily": daily,
             "active_plan": plan, "managed_positions": positions, "position_monitor": monitor})
        if not market.get("allowed") or not daily.get("trusted") or plan or positions:
            print("[WAIT] Новый вход сейчас зависит от условий выше. Runner продолжает мониторинг.")
        if report.get("order_send_allowed"):
            print(
                "[EXECUTION READY] Отправка ордеров разрешена настройками MT5. "
                "Конкретный вход требует сигнала и прохождения risk-gates."
            )
            return 0
        print("[BLOCKED] Торговое исполнение сейчас не готово.")
        return 2
    finally:
        disconnect_mt5()


if __name__ == "__main__":
    raise SystemExit(main())
