from claude_payload import (
    build_claude_payload,
    print_payload_stats,
    save_debug_payload,
)
from market_data import (
    get_market_snapshot,
    print_market_snapshot,
)
from mt5_client import (
    connect_mt5,
    disconnect_mt5,
)


SYMBOL = "XAUUSD"


def main():
    print("=" * 80)
    print("MAX MARKET CONTEXT — SAFE DATA TEST")
    print("=" * 80)
    print("[SAFE TEST] Этот скрипт НЕ вызывает Claude.")
    print("[SAFE TEST] Этот скрипт НЕ использует order_send().")
    print("[SAFE TEST] Risk Manager / Trade State не изменяются.")
    print()

    connected = False

    try:
        connected = connect_mt5()

        if not connected:
            raise RuntimeError(
                "Не удалось подключиться к MT5."
            )

        print()
        print("[INFO] Собираем расширенный Market Snapshot...")

        snapshot = get_market_snapshot(
            symbol=SYMBOL,
        )

        print_market_snapshot(snapshot)

        print()
        print("[INFO] Формируем MAX Market Context для Claude...")

        payload = build_claude_payload(snapshot)

        print_payload_stats(payload)

        save_debug_payload(payload)

        print()
        print("=" * 80)
        print("[OK] MAX MARKET CONTEXT TEST COMPLETED")
        print("=" * 80)
        print(
            "[OK] debug/claude_market_payload.json "
            "пересобран в новом формате."
        )
        print(
            "[SAFE TEST] Торговые действия не выполнялись."
        )
        print("=" * 80)

    finally:
        if connected:
            disconnect_mt5()


if __name__ == "__main__":
    main()
