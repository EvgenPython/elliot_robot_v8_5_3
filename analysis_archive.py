import json
from observability import VERSION, RUN_ID, emit
import os
import time
import uuid
from datetime import datetime
from pathlib import Path

from prop_time import FUNDINGPIPS_TZ
from trade_state import extract_latest_closed_h1_time


BASE_DIR = Path(__file__).resolve().parent
ARCHIVE_DIR = BASE_DIR / "analysis_archive"

ARCHIVE_SCHEMA_VERSION = 6
ARCHIVE_ID_NAMESPACE = uuid.UUID(
    "8f88c066-4564-4b23-87e1-2b3e3ad24b0e"
)
FILE_IO_RETRY_DELAYS_SECONDS = (0.05, 0.15, 0.35, 0.75)


def _json_safe(value):
    """Возвращает JSON-совместимую копию диагностических данных."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, dict):
        return {
            str(key): _json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]

    as_dict = getattr(value, "_asdict", None)
    if callable(as_dict):
        return _json_safe(as_dict())

    return str(value)


def _atomic_write_json(path: Path, data: dict):
    """Пишет JSON атомарно, чтобы publisher не прочитал половину файла."""
    temporary_path = path.with_suffix(path.suffix + ".tmp")

    with open(temporary_path, "w", encoding="utf-8") as file:
        json.dump(
            _json_safe(data),
            file,
            ensure_ascii=False,
            indent=2,
        )
        file.flush()
        os.fsync(file.fileno())

    for attempt_number in range(len(FILE_IO_RETRY_DELAYS_SECONDS) + 1):
        try:
            os.replace(temporary_path, path)
            return
        except OSError:
            if attempt_number >= len(FILE_IO_RETRY_DELAYS_SECONDS):
                raise
            time.sleep(FILE_IO_RETRY_DELAYS_SECONDS[attempt_number])


def _read_json_object(path: Path) -> dict:
    """Tolerates short Windows sharing locks without declaring corruption."""
    last_error = None
    for attempt_number in range(len(FILE_IO_RETRY_DELAYS_SECONDS) + 1):
        try:
            with open(path, "r", encoding="utf-8") as file:
                value = json.load(file)
            if not isinstance(value, dict):
                raise RuntimeError(f"Analysis archive должен быть object: {path}")
            return value
        except (OSError, json.JSONDecodeError) as error:
            last_error = error
            if attempt_number >= len(FILE_IO_RETRY_DELAYS_SECONDS):
                raise
            time.sleep(FILE_IO_RETRY_DELAYS_SECONDS[attempt_number])
    raise last_error


def _build_archive_ids(snapshot: dict, cycle_type: str) -> tuple[str, str]:
    instrument = str(snapshot.get("instrument", "XAUUSD"))
    h1_time = str(extract_latest_closed_h1_time(snapshot) or "unknown_h1")
    snapshot_time = str(snapshot.get("generated_at_fp") or "unknown_time")
    normalized_cycle_type = str(cycle_type).upper()

    cycle_id = str(
        uuid.uuid5(
            ARCHIVE_ID_NAMESPACE,
            f"{instrument}|{h1_time}",
        )
    )
    event_id = str(
        uuid.uuid5(
            ARCHIVE_ID_NAMESPACE,
            f"{cycle_id}|{normalized_cycle_type}|{snapshot_time}",
        )
    )

    return cycle_id, event_id


def _as_fp_datetime(value) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=FUNDINGPIPS_TZ)
    else:
        dt = dt.astimezone(FUNDINGPIPS_TZ)

    return dt


def _safe_h1_label(snapshot: dict) -> str:
    value = extract_latest_closed_h1_time(snapshot)

    if not value:
        return "unknown_h1"

    dt = _as_fp_datetime(value)
    return dt.strftime("%Y%m%d_%H00")


def _archive_path(snapshot: dict, cycle_type: str) -> Path:
    generated = _as_fp_datetime(snapshot["generated_at_fp"])
    day_dir = ARCHIVE_DIR / generated.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)

    filename = (
        f"{generated.strftime('%H%M%S')}_"
        f"{cycle_type.lower()}_"
        f"{_safe_h1_label(snapshot)}.json"
    )

    return day_dir / filename


def save_analysis_archive(
    snapshot: dict,
    cycle_type: str,
    payload: dict | None = None,
    result: dict | None = None,
    scout_result: dict | None = None,
    previous_reference: dict | None = None,
    api_usage: dict | None = None,
    api_attempt: dict | None = None,
    note: str | None = None,
) -> Path:
    """
    Сохраняет точный вход/выход analysis cycle для последующего аудита.

    Credentials сюда не попадают: payload формируется только из рыночных
    данных и предыдущего анализа.
    """

    path = _archive_path(snapshot, cycle_type)

    cycle_id, event_id = _build_archive_ids(
        snapshot,
        cycle_type,
    )

    saved_at_fp = datetime.now(FUNDINGPIPS_TZ).isoformat()

    record = {
        "robot_version": VERSION,
        "run_id": RUN_ID,
        "archive_schema_version": ARCHIVE_SCHEMA_VERSION,
        "event_id": event_id,
        "cycle_id": cycle_id,
        "revision": 1,
        "cycle_type": cycle_type,
        "instrument": snapshot.get("instrument", "XAUUSD"),
        "snapshot_time_fp": snapshot.get("generated_at_fp"),
        "h1_closed_bar_time_fp": extract_latest_closed_h1_time(snapshot),
        "saved_at_fp": saved_at_fp,
        "updated_at_fp": saved_at_fp,
        "note": note,
        "payload": payload,
        "scout_result": scout_result,
        "previous_reference": previous_reference,
        "staged_analysis_version": None,
        "market_map_result": None,
        "market_map_usage": None,
        "market_map_invalid_result": None,
        "market_map_validation_error": None,
        "market_map_repair_usage": None,
        "trade_decision_result": None,
        "trade_decision_usage": None,
        "trade_decision_invalid_result": None,
        "trade_decision_validation_error": None,
        "trade_decision_repair_usage": None,
        "api_usage": api_usage,
        "api_attempt": api_attempt,
        "api_attempts": [],
        "api_cycle": None,
        "result": result,
        "risk_report": None,
        "trade_state_result": None,
        "analysis_registration": None,
        "execution_report": None,
    }

    _atomic_write_json(path, record)

    return path


def _latest_closed_h1_from_payload(payload: dict):
    live_market = payload.get("live_market")
    if not isinstance(live_market, dict):
        live_market = {}
    live_by_tf = live_market.get("raw_timeframes_since_day_start")
    if not isinstance(live_by_tf, dict):
        live_by_tf = {}
    live_h1 = live_by_tf.get("H1")
    if not isinstance(live_h1, dict):
        live_h1 = {}

    candidates = live_h1.get("closed_bars_since_day_start")
    if isinstance(candidates, list):
        for item in reversed(candidates):
            if isinstance(item, dict) and item.get("time"):
                return item["time"]

    history = payload.get("cacheable_history")
    if not isinstance(history, dict):
        history = {}
    history_by_tf = history.get("closed_market_history_before_day_start")
    if not isinstance(history_by_tf, dict):
        history_by_tf = {}
    history_h1 = history_by_tf.get("H1")
    if not isinstance(history_h1, dict):
        history_h1 = {}
    candidates = history_h1.get("closed_bars")
    if isinstance(candidates, list):
        for item in reversed(candidates):
            if isinstance(item, dict) and item.get("time"):
                return item["time"]

    return None


def save_payload_recovery_archive(
    payload: dict,
    cycle_type: str = "FULL_RECOVERY",
    note: str | None = None,
) -> Path:
    """Wraps an already prepared FULL payload without calling Claude/MT5."""
    if not isinstance(payload, dict):
        raise RuntimeError("Recovery payload должен быть JSON object.")

    instrument = str(payload.get("instrument") or "XAUUSD")
    snapshot_time = (
        payload.get("timestamp")
        or (
            payload.get("live_market", {}).get("generated_at_fp")
            if isinstance(payload.get("live_market"), dict)
            else None
        )
    )
    if not snapshot_time:
        raise RuntimeError("Recovery payload не содержит timestamp.")

    generated = _as_fp_datetime(snapshot_time)
    h1_time = _latest_closed_h1_from_payload(payload)
    normalized_cycle_type = str(cycle_type).upper()
    h1_id = str(h1_time or "unknown_h1")
    cycle_id = str(
        uuid.uuid5(
            ARCHIVE_ID_NAMESPACE,
            f"{instrument}|{h1_id}",
        )
    )
    event_id = str(
        uuid.uuid5(
            ARCHIVE_ID_NAMESPACE,
            f"{cycle_id}|{normalized_cycle_type}|{snapshot_time}",
        )
    )

    day_dir = ARCHIVE_DIR / generated.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    h1_label = "unknown_h1"
    if h1_time:
        h1_label = _as_fp_datetime(h1_time).strftime("%Y%m%d_%H00")
    path = day_dir / (
        f"{generated.strftime('%H%M%S')}_"
        f"{normalized_cycle_type.lower()}_{h1_label}.json"
    )

    saved_at_fp = datetime.now(FUNDINGPIPS_TZ).isoformat()
    record = {
        "robot_version": VERSION,
        "run_id": RUN_ID,
        "archive_schema_version": ARCHIVE_SCHEMA_VERSION,
        "event_id": event_id,
        "cycle_id": cycle_id,
        "revision": 1,
        "cycle_type": normalized_cycle_type,
        "instrument": instrument,
        "snapshot_time_fp": str(snapshot_time),
        "h1_closed_bar_time_fp": h1_time,
        "saved_at_fp": saved_at_fp,
        "updated_at_fp": saved_at_fp,
        "note": note or (
            "Восстановлен сохранённый FULL payload; ответ Claude отсутствует, "
            "повторный API-запрос не выполнялся."
        ),
        "payload": payload,
        "scout_result": None,
        "previous_reference": None,
        "staged_analysis_version": None,
        "market_map_result": None,
        "market_map_usage": None,
        "market_map_invalid_result": None,
        "market_map_validation_error": None,
        "market_map_repair_usage": None,
        "trade_decision_result": None,
        "trade_decision_usage": None,
        "trade_decision_invalid_result": None,
        "trade_decision_validation_error": None,
        "trade_decision_repair_usage": None,
        "api_usage": None,
        "api_attempt": {
            "status": "PAYLOAD_RECOVERED_NO_RESPONSE",
            "retry_policy": "NOT_APPLICABLE_RECOVERY_ONLY",
        },
        "api_attempts": [],
        "api_cycle": None,
        "result": None,
        "risk_report": None,
        "trade_state_result": None,
        "analysis_registration": None,
        "execution_report": None,
    }
    _atomic_write_json(path, record)
    return path


def load_analysis_archive(path: Path | str) -> dict:
    archive_path = Path(path)
    return _read_json_object(archive_path)


def update_analysis_archive(
    path: Path | str,
    **sections,
) -> dict:
    """
    Дополняет уже сохранённый analysis cycle результатами Risk Manager,
    Trade State и Executor. Stable event_id позволяет Linux делать upsert.
    """
    archive_path = Path(path)

    record = _read_json_object(archive_path)

    for key, value in sections.items():
        record[str(key)] = _json_safe(value)

    record["revision"] = int(record.get("revision", 1)) + 1
    record["updated_at_fp"] = datetime.now(FUNDINGPIPS_TZ).isoformat()

    _atomic_write_json(archive_path, record)
    return record


def safe_update_analysis_archive(
    path: Path | str,
    **sections,
) -> bool:
    """
    Best-effort telemetry update. Ошибка веб-архива никогда не блокирует
    Risk Manager, Trade State или торговое исполнение.
    """
    try:
        update_analysis_archive(path, **sections)
        emit("archive", "updated", data={"path": str(path), "sections": list(sections)})
        return True
    except Exception as error:
        print(
            "[ARCHIVE WARNING] Не удалось дополнить веб-архив; "
            "торговый цикл продолжается: "
            f"{type(error).__name__}: {error}"
        )
        return False
