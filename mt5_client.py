import json
from pathlib import Path

import MetaTrader5 as mt5


BASE_DIR = Path(__file__).resolve().parent
ACCOUNT_CONFIG_PATH = BASE_DIR / "config" / "account.json"


def load_account_config() -> dict:
    """
    Загружает настройки торгового счёта из config/account.json.
    """

    if not ACCOUNT_CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Не найден файл конфигурации:\n{ACCOUNT_CONFIG_PATH}"
        )

    with open(ACCOUNT_CONFIG_PATH, "r", encoding="utf-8") as file:
        config = json.load(file)

    required_fields = [
        "login",
        "password",
        "server",
        "mt5_path",
    ]

    missing_fields = [
        field
        for field in required_fields
        if field not in config
    ]

    if missing_fields:
        raise ValueError(
            "В account.json отсутствуют поля: "
            + ", ".join(missing_fields)
        )

    return config


def connect_mt5() -> bool:
    """
    Подключается к конкретному терминалу MT5
    и конкретному торговому счёту.
    """

    config = load_account_config()

    login = int(config["login"])
    password = str(config["password"])
    server = str(config["server"])
    mt5_path = str(config["mt5_path"])

    print("=" * 70)
    print("ПОДКЛЮЧЕНИЕ К META TRADER 5")
    print("=" * 70)

    print(f"[INFO] Login:    {login}")
    print(f"[INFO] Server:   {server}")
    print(f"[INFO] Terminal: {mt5_path}")
    print()

    if not Path(mt5_path).is_file():
        print("[ERROR] Файл terminal64.exe не найден:")
        print(mt5_path)
        return False

    initialized = mt5.initialize(
        mt5_path,
        login=login,
        password=password,
        server=server,
        timeout=60_000,
    )

    if not initialized:
        print("[ERROR] Не удалось подключиться к MetaTrader 5")
        print(f"[ERROR] MT5 last_error(): {mt5.last_error()}")
        return False

    account = mt5.account_info()

    if account is None:
        print("[ERROR] Не удалось получить данные торгового счёта.")
        print(f"[ERROR] MT5 last_error(): {mt5.last_error()}")

        mt5.shutdown()
        return False

    # Проверяем, что подключились именно к нужному счёту.
    if int(account.login) != int(login):
        print("[ERROR] MT5 подключён к другому счёту.")
        print(f"[ERROR] Ожидался login: {login}")
        print(f"[ERROR] Получен login:  {account.login}")

        mt5.shutdown()
        return False

    # Проверяем не только login, но и server.
    # Это обязательная защита перед LIVE execution.
    actual_server = str(account.server).strip()
    expected_server = str(server).strip()

    if actual_server.casefold() != expected_server.casefold():
        print("[ERROR] MT5 подключён к другому серверу.")
        print(f"[ERROR] Ожидался server: {expected_server}")
        print(f"[ERROR] Получен server:  {actual_server}")

        mt5.shutdown()
        return False

    terminal = mt5.terminal_info()
    version = mt5.version()

    print("[OK] Соединение с MT5 установлено.")
    print()

    print("ТОРГОВЫЙ СЧЁТ")
    print("-" * 70)
    print(f"Login:        {account.login}")
    print(f"Server:       {account.server}")
    print(f"Broker:       {account.company}")
    print(f"Name:         {account.name}")
    print(f"Currency:     {account.currency}")
    print(f"Leverage:     1:{account.leverage}")
    print(f"Balance:      {account.balance:.2f}")
    print(f"Equity:       {account.equity:.2f}")
    print(f"Margin:       {account.margin:.2f}")
    print(f"Free margin:  {account.margin_free:.2f}")

    print()
    print("ТЕРМИНАЛ")
    print("-" * 70)

    if version is not None:
        print(f"MT5 version:  {version}")

    if terminal is not None:
        print(f"Connected:    {terminal.connected}")
        print(f"Trade allowed:{terminal.trade_allowed}")

    print("=" * 70)

    return True


def disconnect_mt5():
    """
    Закрывает соединение Python с MT5.
    """

    mt5.shutdown()
    print("[INFO] Соединение с MT5 закрыто.")
