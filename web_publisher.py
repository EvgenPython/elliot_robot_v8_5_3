"""
Односторонний read-only publisher: Windows robot -> Linux web backend.

Этот процесс не импортируется торговым роботом, не обращается к Claude/MT5
и не принимает команды с Linux. Он только читает локальные JSON-файлы и
отправляет их HTTPS POST-запросами с идемпотентными идентификаторами.
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from instruments import active_instrument, symbol_state_path
from single_instance import SingleInstanceError, SingleInstanceLock


BASE_DIR = Path(__file__).resolve().parent
ARCHIVE_DIR = BASE_DIR / "analysis_archive"
STATE_DIR = BASE_DIR / "state"
MARKET_SNAPSHOT_PATH = symbol_state_path("web_market_snapshot.json")
DEFAULT_CONFIG_PATH = BASE_DIR / "config" / "web_export.json"
PUBLISHER_STATE_PATH = symbol_state_path("web_publisher_state.json")
_PUBLISHER_SYMBOL = "".join(
    character if character.isalnum() else "_"
    for character in active_instrument()
)
_COMMON_LOCK_ROOT = Path(os.environ.get("PROGRAMDATA", str(BASE_DIR)))
PUBLISHER_LOCK_PATH = (
    _COMMON_LOCK_ROOT
    / "WaveFrame"
    / f"web_publisher_{_PUBLISHER_SYMBOL}.lock"
)

PUBLISH_NAMESPACE = uuid.UUID("66e91a8e-b0af-4530-9ed7-70f470e9ea25")
ENGINE_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,100}$")

RUNTIME_SOURCE_PATHS = {
    "runner_status": symbol_state_path("runner_status.json"),
    "analysis_state": symbol_state_path("analysis_state.json"),
    "trade_state": symbol_state_path("trade_state.json"),
    "fundingpips_risk_state": symbol_state_path("fundingpips_risk_state.json"),
    "claude_reference_state": symbol_state_path("claude_reference_state.json"),
    "claude_request_guard": symbol_state_path("claude_request_guard.json"),
    "claude_resilient_pipeline": symbol_state_path("claude_resilient_pipeline.json"),
    "entry_watch": symbol_state_path("entry_watch.json"),
    "position_monitor": symbol_state_path("position_monitor_state.json"),
    "position_protection": symbol_state_path("position_protection_state.json"),
}

DEFAULTS = {
    "enabled": False,
    "base_url": "",
    "engine_id": "",
    "api_token": "",
    "api_token_env": "ROBOT_WEB_API_TOKEN",
    "analysis_endpoint_path": "/api/v1/robot/analysis-events",
    "market_endpoint_path": "/api/v1/robot/market-snapshot",
    "runtime_endpoint_path": "/api/v1/robot/runtime-state",
    "poll_interval_seconds": 5,
    "runtime_interval_seconds": 15,
    "timeout_seconds": 10,
    "max_archive_events_per_pass": 20,
}


class PublisherError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        value = json.load(file)

    if not isinstance(value, dict):
        raise PublisherError(f"JSON root должен быть object: {path}")

    return value


def _read_optional_json(path: Path) -> dict | None:
    if not path.exists():
        return None

    try:
        return _read_json(path)
    except (OSError, json.JSONDecodeError, PublisherError):
        return None


def _atomic_write_json(path: Path, value: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")

    with open(temporary_path, "w", encoding="utf-8") as file:
        json.dump(
            value,
            file,
            ensure_ascii=False,
            indent=2,
        )
        file.flush()
        os.fsync(file.fileno())

    os.replace(temporary_path, path)


def load_config(path: Path) -> dict:
    if not path.exists():
        raise PublisherError(
            "Не найден config/web_export.json. "
            "Скопируйте web_export.example.json и заполните параметры."
        )

    raw = _read_json(path)
    config = dict(DEFAULTS)
    config.update(raw)

    if not bool(config.get("enabled")):
        return config

    base_url = str(config.get("base_url", "")).strip().rstrip("/")
    if not base_url.startswith("https://"):
        raise PublisherError("base_url должен начинаться с https://")
    config["base_url"] = base_url

    engine_id = str(config.get("engine_id", "")).strip()
    if not ENGINE_ID_PATTERN.fullmatch(engine_id):
        raise PublisherError(
            "engine_id: только A-Z, a-z, 0-9, точка, '_' и '-', длина 1..100."
        )
    config["engine_id"] = engine_id

    for key in (
        "analysis_endpoint_path",
        "market_endpoint_path",
        "runtime_endpoint_path",
    ):
        value = str(config.get(key, "")).strip()
        if not value.startswith("/"):
            raise PublisherError(f"{key} должен начинаться с '/'.")
        config[key] = value

    numeric_limits = {
        "poll_interval_seconds": (2, 3600),
        "runtime_interval_seconds": (5, 3600),
        "timeout_seconds": (1, 60),
        "max_archive_events_per_pass": (1, 100),
    }
    for key, (minimum, maximum) in numeric_limits.items():
        try:
            value = int(config.get(key))
        except (TypeError, ValueError) as error:
            raise PublisherError(f"{key} должен быть целым числом.") from error
        if value < minimum or value > maximum:
            raise PublisherError(
                f"{key} должен быть в диапазоне {minimum}..{maximum}."
            )
        config[key] = value

    token_env = str(config.get("api_token_env", "")).strip()
    token = os.environ.get(token_env, "").strip() if token_env else ""
    if not token:
        token = str(config.get("api_token", "")).strip()
    if not token:
        raise PublisherError(
            "Не задан API token: установите переменную api_token_env "
            "или заполните api_token в локальном web_export.json."
        )
    config["resolved_api_token"] = token

    return config


def load_publisher_state() -> dict:
    state = _read_optional_json(PUBLISHER_STATE_PATH)
    if not isinstance(state, dict):
        state = {}

    delivered = state.get("delivered_archives")
    if not isinstance(delivered, dict):
        delivered = {}

    return {
        "schema_version": 2,
        "updated_at_utc": state.get("updated_at_utc"),
        "delivered_archives": delivered,
        "market_content_sha256": state.get("market_content_sha256"),
        "market_source_snapshot_at_fp": state.get(
            "market_source_snapshot_at_fp"
        ),
        "last_market_sent_at_utc": state.get("last_market_sent_at_utc"),
        "last_runtime_sent_at_utc": state.get("last_runtime_sent_at_utc"),
    }


def save_publisher_state(state: dict):
    state["schema_version"] = 2
    state["updated_at_utc"] = _utc_now()
    _atomic_write_json(PUBLISHER_STATE_PATH, state)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        while True:
            chunk = file.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _fallback_event_id(engine_id: str, relative_path: str) -> str:
    return str(
        uuid.uuid5(
            PUBLISH_NAMESPACE,
            f"{engine_id}|archive|{relative_path}",
        )
    )


def build_analysis_envelope(
    config: dict,
    record: dict,
    relative_path: str,
    content_sha256: str,
) -> dict:
    event_id = str(
        record.get("event_id")
        or _fallback_event_id(config["engine_id"], relative_path)
    )

    return {
        "contract_version": 1,
        "kind": "analysis_event",
        "engine_id": config["engine_id"],
        "event_id": event_id,
        "cycle_id": record.get("cycle_id"),
        "event_revision": int(record.get("revision", 1) or 1),
        "content_sha256": content_sha256,
        "source_path": relative_path,
        "sent_at_utc": _utc_now(),
        "payload": record,
    }


def build_runtime_envelope(config: dict) -> dict:
    sources = {}
    missing_sources = []

    for name, path in RUNTIME_SOURCE_PATHS.items():
        value = _read_optional_json(path)
        sources[name] = value
        if value is None:
            missing_sources.append(name)

    runner_status = sources.get("runner_status") or {}
    runtime_id = str(
        uuid.uuid5(
            PUBLISH_NAMESPACE,
            f"{config['engine_id']}|runtime",
        )
    )

    return {
        "contract_version": 1,
        "kind": "runtime_state",
        "engine_id": config["engine_id"],
        "runtime_id": runtime_id,
        "source_generated_at_fp": runner_status.get("generated_at_fp"),
        "sent_at_utc": _utc_now(),
        "missing_sources": missing_sources,
        "payload": sources,
    }


def build_market_envelope(
    config: dict,
    payload: dict,
    content_sha256: str,
) -> dict:
    source_snapshot_at_fp = (
        payload.get("timestamp")
        or (
            payload.get("live_market", {}).get("generated_at_fp")
            if isinstance(payload.get("live_market"), dict)
            else None
        )
    )
    if not source_snapshot_at_fp:
        raise PublisherError("Market snapshot не содержит timestamp.")

    snapshot_id = str(
        uuid.uuid5(
            PUBLISH_NAMESPACE,
            f"{config['engine_id']}|market-snapshot",
        )
    )
    return {
        "contract_version": 1,
        "kind": "market_snapshot",
        "engine_id": config["engine_id"],
        "snapshot_id": snapshot_id,
        "source_snapshot_at_fp": str(source_snapshot_at_fp),
        "content_sha256": content_sha256,
        "sent_at_utc": _utc_now(),
        "payload": payload,
    }


def _post_json(
    config: dict,
    endpoint_path: str,
    envelope: dict,
    idempotency_key: str,
):
    body = json.dumps(
        envelope,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    request = Request(
        url=config["base_url"] + endpoint_path,
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {config['resolved_api_token']}",
            "Content-Type": "application/json; charset=utf-8",
            "Idempotency-Key": idempotency_key,
            "User-Agent": "robot-elliot-web-publisher/1.0",
            "X-Robot-Engine-ID": config["engine_id"],
        },
    )

    try:
        with urlopen(
            request,
            timeout=config["timeout_seconds"],
        ) as response:
            status = int(getattr(response, "status", 0) or 0)
            response.read(1024)
    except HTTPError as error:
        raise PublisherError(
            f"Linux API вернул HTTP {error.code}."
        ) from error
    except (URLError, TimeoutError, OSError) as error:
        raise PublisherError(
            f"Linux API недоступен: {type(error).__name__}: {error}"
        ) from error

    if status < 200 or status >= 300:
        raise PublisherError(f"Linux API вернул HTTP {status}.")


def send_pending_archives(config: dict, state: dict) -> tuple[int, bool]:
    if not ARCHIVE_DIR.exists():
        return 0, False

    sent_count = 0
    failed = False
    delivered = state["delivered_archives"]
    candidates = sorted(ARCHIVE_DIR.rglob("*.json"))

    for path in candidates:
        if sent_count >= config["max_archive_events_per_pass"]:
            break

        relative_path = path.relative_to(BASE_DIR).as_posix()

        try:
            content_sha256 = _file_sha256(path)
        except OSError as error:
            print(f"[PUBLISHER WARNING] Не удалось прочитать {relative_path}: {error}")
            continue

        previous = delivered.get(relative_path, {})
        if previous.get("content_sha256") == content_sha256:
            continue

        try:
            record = _read_json(path)
            envelope = build_analysis_envelope(
                config,
                record,
                relative_path,
                content_sha256,
            )
            idempotency_key = (
                f"{config['engine_id']}:analysis:"
                f"{envelope['event_id']}:{content_sha256}"
            )
            _post_json(
                config,
                config["analysis_endpoint_path"],
                envelope,
                idempotency_key,
            )
        except (OSError, json.JSONDecodeError, PublisherError) as error:
            print(
                f"[PUBLISHER ERROR] {relative_path} не отправлен: "
                f"{type(error).__name__}: {error}"
            )
            failed = True
            break

        delivered[relative_path] = {
            "content_sha256": content_sha256,
            "event_id": envelope["event_id"],
            "event_revision": envelope["event_revision"],
            "sent_at_utc": envelope["sent_at_utc"],
        }
        save_publisher_state(state)
        sent_count += 1
        print(
            f"[PUBLISHER] Analysis отправлен: {relative_path} "
            f"(revision {envelope['event_revision']})."
        )

    return sent_count, failed


def send_market_snapshot(config: dict, state: dict) -> bool:
    """Sends raw candles independently from Claude analysis archives."""
    if not MARKET_SNAPSHOT_PATH.exists():
        return False

    try:
        content_sha256 = _file_sha256(MARKET_SNAPSHOT_PATH)
        if state.get("market_content_sha256") == content_sha256:
            return False

        payload = _read_json(MARKET_SNAPSHOT_PATH)
        envelope = build_market_envelope(config, payload, content_sha256)
        _post_json(
            config,
            config["market_endpoint_path"],
            envelope,
            (
                f"{config['engine_id']}:market:"
                f"{envelope['snapshot_id']}:{content_sha256}"
            ),
        )
    except (OSError, json.JSONDecodeError, PublisherError) as error:
        raise PublisherError(
            "Market snapshot не отправлен: "
            f"{type(error).__name__}: {error}"
        ) from error

    state["market_content_sha256"] = content_sha256
    state["market_source_snapshot_at_fp"] = envelope[
        "source_snapshot_at_fp"
    ]
    state["last_market_sent_at_utc"] = envelope["sent_at_utc"]
    save_publisher_state(state)
    print(
        "[PUBLISHER] Market snapshot отправлен: "
        f"{envelope['source_snapshot_at_fp']}."
    )
    return True


def send_runtime_state(config: dict, state: dict):
    envelope = build_runtime_envelope(config)
    source_time = envelope.get("source_generated_at_fp") or "no-runner-heartbeat"
    idempotency_key = (
        f"{config['engine_id']}:runtime:{source_time}:"
        f"{envelope['sent_at_utc']}"
    )
    _post_json(
        config,
        config["runtime_endpoint_path"],
        envelope,
        idempotency_key,
    )
    state["last_runtime_sent_at_utc"] = envelope["sent_at_utc"]
    save_publisher_state(state)
    print("[PUBLISHER] Runtime state отправлен.")


def run(config_path: Path, once: bool = False) -> int:
    try:
        config = load_config(config_path)
    except (OSError, json.JSONDecodeError, PublisherError) as error:
        print(f"[PUBLISHER CONFIG ERROR] {error}")
        return 2

    if not bool(config.get("enabled")):
        print("[PUBLISHER] enabled=false; отправка выключена.")
        return 0

    state = load_publisher_state()
    print(
        f"[PUBLISHER] Односторонняя отправка включена: "
        f"engine_id={config['engine_id']} -> {config['base_url']}"
    )

    next_runtime_at = 0.0
    had_error = False

    while True:
        try:
            send_market_snapshot(config, state)
        except Exception as error:
            had_error = True
            print(
                "[PUBLISHER ERROR] Market snapshot не отправлен: "
                f"{type(error).__name__}: {error}"
            )

        try:
            _, queue_failed = send_pending_archives(config, state)
            had_error = had_error or queue_failed
        except Exception as error:
            had_error = True
            print(
                "[PUBLISHER ERROR] Ошибка очереди analysis: "
                f"{type(error).__name__}: {error}"
            )

        now_monotonic = time.monotonic()
        if now_monotonic >= next_runtime_at:
            try:
                send_runtime_state(config, state)
            except Exception as error:
                had_error = True
                print(
                    "[PUBLISHER ERROR] Runtime state не отправлен: "
                    f"{type(error).__name__}: {error}"
                )
            next_runtime_at = now_monotonic + config["runtime_interval_seconds"]

        if once:
            return 1 if had_error else 0

        time.sleep(config["poll_interval_seconds"])


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        description="Read-only Windows -> Linux publisher для robot_elliot."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Путь к локальному web_export.json.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Один проход очереди и один runtime snapshot, затем выход.",
    )
    return parser.parse_args(argv)


def main() -> int:
    from console_log import install_console_log
    from observability import start_session
    install_console_log("publisher")
    start_session("publisher")
    args = parse_args()
    try:
        with SingleInstanceLock(PUBLISHER_LOCK_PATH):
            return run(args.config, once=args.once)
    except SingleInstanceError as error:
        print("[PUBLISHER BLOCKED] Вторая копия publisher не запущена.")
        print(str(error))
        return 3


if __name__ == "__main__":
    sys.exit(main())
