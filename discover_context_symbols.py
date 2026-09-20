import MetaTrader5 as mt5

from mt5_client import (
    connect_mt5,
    disconnect_mt5,
)


# Ищем потенциальные context-инструменты у текущего брокера.
# Ничего не выбираем автоматически и ничего не торгуем.
KEYWORDS = [
    "XAG",
    "SILVER",
    "DXY",
    "USDX",
    "DOLLAR INDEX",
    "USD INDEX",
    "US10",
    "10Y",
    "TREASURY",
    "BOND",
    "UST",
    "US500",
    "SP500",
    "S&P",
    "NAS100",
    "NASDAQ",
    "US100",
    "VIX",
    "VOLATILITY",
    "USDJPY",
    "EURUSD",
]


def main():
    print("=" * 80)
    print("CONTEXT SYMBOL DISCOVERY — SAFE TEST")
    print("=" * 80)
    print("[SAFE TEST] Скрипт только читает список символов MT5.")
    print("[SAFE TEST] order_send() не используется.")
    print()

    connected = False

    try:
        connected = connect_mt5()

        if not connected:
            raise RuntimeError(
                "Не удалось подключиться к MT5."
            )

        symbols = mt5.symbols_get()

        if not symbols:
            raise RuntimeError(
                "MT5 не вернул список символов."
            )

        matches = []

        for item in symbols:
            name = str(
                getattr(item, "name", "")
            )

            description = str(
                getattr(item, "description", "")
            )

            path = str(
                getattr(item, "path", "")
            )

            haystack = (
                f"{name} {description} {path}"
            ).upper()

            matched_keywords = [
                keyword
                for keyword in KEYWORDS
                if keyword.upper() in haystack
            ]

            if not matched_keywords:
                continue

            matches.append(
                (
                    name,
                    description,
                    path,
                    ", ".join(matched_keywords),
                )
            )

        print(
            f"Всего символов у брокера: {len(symbols)}"
        )
        print(
            f"Найдено context-кандидатов: {len(matches)}"
        )
        print()

        if not matches:
            print(
                "Подходящих кандидатов по ключевым словам не найдено."
            )
            return

        for (
            name,
            description,
            path,
            keywords,
        ) in sorted(matches):
            print("-" * 80)
            print(f"Symbol:      {name}")
            print(f"Description: {description}")
            print(f"Path:        {path}")
            print(f"Matched:     {keywords}")

        print("-" * 80)
        print()
        print(
            "[INFO] Ничего автоматически в стратегию не добавлено."
        )

    finally:
        if connected:
            disconnect_mt5()


if __name__ == "__main__":
    main()
