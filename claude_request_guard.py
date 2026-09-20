"""Durable attempt journal for Claude API calls.

The journal separates two concerns that must never be mixed:

* a missing/invalid Claude response may be retried a bounded number of times;
* only one locally validated response may win for a H1/stage and continue to
  Risk Manager / Executor.

Every attempt is persisted before the paid request starts. If Windows or the
runner stops mid-stream, the interrupted attempt remains auditable and the
next runner start may consume the next configured retry slot.
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from instruments import symbol_state_path


BASE_DIR = Path(__file__).resolve().parent
STATE_PATH = symbol_state_path("claude_request_guard.json")
STATE_VERSION = 3
MAX_CYCLES = 200
FILE_IO_RETRY_DELAYS_SECONDS = (0.05, 0.15, 0.35, 0.75)

ACTIVE_ATTEMPT_STATUSES = {
    "DISPATCH_STARTED",
    "PREFLIGHT_COMPLETED",
}

FINAL_CYCLE_STATUSES = {
    "VALIDATED",
    "EXHAUSTED",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _attempt_key(h1_closed_bar_time, api_stage: str) -> str:
    return f"{str(h1_closed_bar_time)}|{str(api_stage).strip().upper()}"


def _empty_state() -> dict:
    return {
        "version": STATE_VERSION,
        "updated_at_utc": None,
        "cycles": {},
    }


def _migrate_legacy_state(state: dict) -> dict:
    """Converts the former one-attempt/no-retry file without losing audit."""
    migrated = _empty_state()
    legacy_attempts = state.get("attempts")
    if not isinstance(legacy_attempts, dict):
        return migrated

    now = _utc_now()
    for key, source in legacy_attempts.items():
        if not isinstance(source, dict):
            continue

        attempt = dict(source)
        old_status = str(attempt.get("status") or "").upper()
        attempt["attempt_number"] = 1
        attempt["legacy_status"] = old_status or None
        attempt["retryable"] = old_status != "COMPLETED"
        attempt["outcome_unknown"] = old_status != "COMPLETED"
        attempt["no_retry"] = False

        if old_status == "COMPLETED":
            attempt["status"] = "VALIDATED"
            cycle_status = "VALIDATED"
            winner_attempt_id = attempt.get("attempt_id")
        else:
            attempt["status"] = "FAILED_PROCESS_INTERRUPTED"
            attempt["completed_at_utc"] = (
                attempt.get("completed_at_utc") or now
            )
            cycle_status = "WAITING_RETRY"
            winner_attempt_id = None

        stage = str(attempt.get("api_stage") or "UNKNOWN").upper()
        h1_time = attempt.get("h1_closed_bar_time")
        migrated["cycles"][str(key)] = {
            "key": str(key),
            "h1_closed_bar_time": h1_time,
            "api_stage": stage,
            "cycle_type": attempt.get("cycle_type"),
            "payload_timestamp": attempt.get("payload_timestamp"),
            "archive_path": attempt.get("archive_path"),
            "max_attempts": 3,
            "status": cycle_status,
            "winner_attempt_id": winner_attempt_id,
            "block_reason": None,
            "alert_required": False,
            "created_at_utc": attempt.get("started_at_utc") or now,
            "updated_at_utc": now,
            "attempts": [attempt],
        }

    return migrated


def _load_state() -> dict:
    state = None
    for attempt_number in range(len(FILE_IO_RETRY_DELAYS_SECONDS) + 1):
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as file:
                state = json.load(file)
            break
        except FileNotFoundError:
            return _empty_state()
        except (OSError, json.JSONDecodeError):
            if attempt_number >= len(FILE_IO_RETRY_DELAYS_SECONDS):
                raise
            time.sleep(FILE_IO_RETRY_DELAYS_SECONDS[attempt_number])

    if not isinstance(state, dict):
        raise RuntimeError("claude_request_guard.json должен содержать object.")

    if not isinstance(state.get("cycles"), dict):
        return _migrate_legacy_state(state)

    return {
        "version": STATE_VERSION,
        "updated_at_utc": state.get("updated_at_utc"),
        "cycles": state["cycles"],
    }


def _atomic_save(state: dict):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = STATE_PATH.with_suffix(STATE_PATH.suffix + ".tmp")
    state["version"] = STATE_VERSION
    state["updated_at_utc"] = _utc_now()

    with open(temporary_path, "w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False, indent=2)
        file.flush()
        os.fsync(file.fileno())

    for attempt_number in range(len(FILE_IO_RETRY_DELAYS_SECONDS) + 1):
        try:
            os.replace(temporary_path, STATE_PATH)
            return
        except OSError:
            if attempt_number >= len(FILE_IO_RETRY_DELAYS_SECONDS):
                raise
            time.sleep(FILE_IO_RETRY_DELAYS_SECONDS[attempt_number])


def _prune(state: dict):
    cycles = state["cycles"]
    if len(cycles) <= MAX_CYCLES:
        return

    ordered = sorted(
        cycles.items(),
        key=lambda item: str(item[1].get("updated_at_utc", "")),
        reverse=True,
    )
    state["cycles"] = dict(ordered[:MAX_CYCLES])


def _copy(value):
    return json.loads(json.dumps(value, ensure_ascii=False))


def _find_attempt(cycle: dict, attempt_id: str | None) -> dict:
    attempts = cycle.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        raise RuntimeError("API attempt journal пуст.")

    if attempt_id in (None, ""):
        return attempts[-1]

    for attempt in attempts:
        if str(attempt.get("attempt_id")) == str(attempt_id):
            return attempt

    raise RuntimeError(f"API attempt не найден: {attempt_id}")


def begin_api_attempt(
    h1_closed_bar_time,
    api_stage: str,
    cycle_type: str,
    payload_timestamp=None,
    archive_path=None,
    max_attempts: int = 3,
) -> dict:
    """Persists a new bounded attempt before any paid Messages call."""
    if not h1_closed_bar_time:
        raise RuntimeError("API journal: не определена закрытая H1.")

    max_attempts = max(1, int(max_attempts))
    stage = str(api_stage).strip().upper()
    key = _attempt_key(h1_closed_bar_time, stage)
    state = _load_state()
    now = _utc_now()
    cycle = state["cycles"].get(key)

    if not isinstance(cycle, dict):
        cycle = {
            "key": key,
            "h1_closed_bar_time": str(h1_closed_bar_time),
            "api_stage": stage,
            "cycle_type": str(cycle_type),
            "payload_timestamp": (
                str(payload_timestamp)
                if payload_timestamp not in (None, "")
                else None
            ),
            "archive_path": str(archive_path) if archive_path else None,
            "max_attempts": max_attempts,
            "status": "READY",
            "winner_attempt_id": None,
            "block_reason": None,
            "alert_required": False,
            "created_at_utc": now,
            "updated_at_utc": now,
            "attempts": [],
        }
    else:
        cycle["max_attempts"] = max_attempts
        cycle["archive_path"] = (
            cycle.get("archive_path")
            or (str(archive_path) if archive_path else None)
        )

    if str(cycle.get("status")) == "VALIDATED":
        attempts = cycle.get("attempts") or []
        return {
            "allowed": False,
            "reason": "VALIDATED_RESPONSE_ALREADY_EXISTS",
            "key": key,
            "cycle": _copy(cycle),
            "attempt": _copy(attempts[-1]) if attempts else None,
        }

    if str(cycle.get("status")) == "EXHAUSTED":
        attempts = cycle.get("attempts") or []
        return {
            "allowed": False,
            "reason": (
                "MAX_ATTEMPTS_EXHAUSTED"
                if len(attempts) >= max_attempts
                else "CYCLE_EXHAUSTED"
            ),
            "key": key,
            "cycle": _copy(cycle),
            "attempt": _copy(attempts[-1]) if attempts else None,
        }

    # A surviving active record means the prior process ended before it could
    # close the journal entry. single_instance.py prevents a valid parallel
    # runner from arriving here.
    for old_attempt in cycle.get("attempts", []):
        if old_attempt.get("status") in ACTIVE_ATTEMPT_STATUSES:
            old_attempt["status"] = "FAILED_PROCESS_INTERRUPTED"
            old_attempt["retryable"] = True
            old_attempt["outcome_unknown"] = True
            old_attempt["failure_class"] = "PROCESS_INTERRUPTED"
            old_attempt["billing_status"] = "UNKNOWN_MAY_BE_BILLED"
            old_attempt["alert_required"] = True
            old_attempt["completed_at_utc"] = now
            old_attempt["updated_at_utc"] = now
            old_attempt["error_type"] = "ProcessInterrupted"
            old_attempt["error_message"] = (
                "Runner/process stopped before the API attempt was finalized."
            )

    attempts = cycle.setdefault("attempts", [])
    if len(attempts) >= max_attempts:
        cycle["status"] = "EXHAUSTED"
        cycle["block_reason"] = cycle.get("block_reason") or "ATTEMPT_LIMIT"
        cycle["alert_required"] = True
        cycle["updated_at_utc"] = now
        state["cycles"][key] = cycle
        _atomic_save(state)
        return {
            "allowed": False,
            "reason": "MAX_ATTEMPTS_EXHAUSTED",
            "key": key,
            "cycle": _copy(cycle),
            "attempt": _copy(attempts[-1]) if attempts else None,
        }

    attempt = {
        "attempt_id": str(uuid.uuid4()),
        "attempt_number": len(attempts) + 1,
        "key": key,
        "h1_closed_bar_time": str(h1_closed_bar_time),
        "api_stage": stage,
        "cycle_type": str(cycle_type),
        "payload_timestamp": (
            str(payload_timestamp) if payload_timestamp not in (None, "") else None
        ),
        "archive_path": str(archive_path) if archive_path else None,
        "status": "DISPATCH_STARTED",
        "retryable": None,
        "outcome_unknown": False,
        "started_at_utc": now,
        "updated_at_utc": now,
        "completed_at_utc": None,
        "model": None,
        "input_tokens": None,
        "transport_payload_bytes": None,
        "payload_sha256": None,
        "request_sha256": None,
        "retry_recovery_from": None,
        "request_id": None,
        "response_id": None,
        "stop_reason": None,
        "response_received": False,
        "usage": None,
        "failure_class": None,
        "billing_status": "NOT_DISPATCHED_YET",
        "delivery_recovered": False,
        "stream_journal_path": None,
        "stream_state": None,
        "stream_chunks": 0,
        "stream_received_characters": 0,
        "stream_journal_error": None,
        "alert_required": False,
        "error_type": None,
        "error_message": None,
    }
    attempts.append(attempt)
    cycle["status"] = "IN_PROGRESS"
    cycle["updated_at_utc"] = now
    state["cycles"][key] = cycle
    _prune(state)
    _atomic_save(state)

    return {
        "allowed": True,
        "reason": "ATTEMPT_STARTED",
        "key": key,
        "attempt": _copy(attempt),
        "cycle": _copy(cycle),
    }


def update_api_attempt(
    h1_closed_bar_time,
    api_stage: str,
    attempt_id: str,
    **fields,
) -> dict:
    """Durably enriches an active attempt (token count, request ID, usage)."""
    key = _attempt_key(h1_closed_bar_time, api_stage)
    state = _load_state()
    cycle = state["cycles"].get(key)
    if not isinstance(cycle, dict):
        raise RuntimeError(f"API cycle не найден: {key}")

    attempt = _find_attempt(cycle, attempt_id)
    now = _utc_now()
    allowed_fields = {
        "model",
        "input_tokens",
        "transport_payload_bytes",
        "payload_sha256",
        "request_sha256",
        "retry_recovery_from",
        "request_id",
        "response_id",
        "stop_reason",
        "response_received",
        "usage",
        "billing_status",
        "delivery_recovered",
        "stream_journal_path",
        "stream_state",
        "stream_chunks",
        "stream_received_characters",
        "stream_journal_error",
    }

    incoming_hash = fields.get("payload_sha256")
    if incoming_hash not in (None, ""):
        prior_hashes = {
            str(item.get("payload_sha256"))
            for item in cycle.get("attempts", [])
            if item is not attempt and item.get("payload_sha256")
        }
        if prior_hashes and str(incoming_hash) not in prior_hashes:
            raise RuntimeError(
                "API retry payload_sha256 отличается от первой попытки; "
                "платный запрос с изменённым snapshot запрещён."
            )

    for name, value in fields.items():
        if name in allowed_fields and value is not None:
            attempt[name] = value

    if attempt.get("input_tokens") is not None:
        attempt["status"] = "PREFLIGHT_COMPLETED"
    attempt["updated_at_utc"] = now
    cycle["updated_at_utc"] = now
    state["cycles"][key] = cycle
    _atomic_save(state)
    return _copy(attempt)


def mark_api_attempt(
    h1_closed_bar_time,
    api_stage: str,
    status: str,
    attempt_id: str | None = None,
    request_id=None,
    usage: dict | None = None,
    error=None,
    retryable: bool | None = None,
    outcome_unknown: bool = False,
    failure_class: str | None = None,
    billing_status: str | None = None,
    delivery_recovered: bool | None = None,
) -> dict:
    """Finalizes one attempt and advances the durable cycle state."""
    key = _attempt_key(h1_closed_bar_time, api_stage)
    state = _load_state()
    cycle = state["cycles"].get(key)
    if not isinstance(cycle, dict):
        raise RuntimeError(f"API cycle не найден: {key}")

    normalized_status = str(status).strip().upper()
    if normalized_status not in {
        "FAILED_RETRYABLE",
        "FAILED_PERMANENT",
        "VALIDATED",
    }:
        raise RuntimeError(f"Неизвестный API attempt status: {normalized_status}")

    attempt = _find_attempt(cycle, attempt_id)
    now = _utc_now()
    attempt["status"] = normalized_status
    attempt["retryable"] = (
        normalized_status == "FAILED_RETRYABLE"
        if retryable is None
        else bool(retryable)
    )
    attempt["outcome_unknown"] = bool(outcome_unknown)
    if failure_class not in (None, ""):
        attempt["failure_class"] = str(failure_class)
    if billing_status not in (None, ""):
        attempt["billing_status"] = str(billing_status)
    elif isinstance(usage, dict):
        attempt["billing_status"] = "USAGE_AVAILABLE"
    if delivery_recovered is not None:
        attempt["delivery_recovered"] = bool(delivery_recovered)
    attempt["updated_at_utc"] = now
    attempt["completed_at_utc"] = now
    if request_id not in (None, ""):
        attempt["request_id"] = str(request_id)
    if isinstance(usage, dict):
        attempt["usage"] = dict(usage)
    if error is not None:
        attempt["error_type"] = type(error).__name__
        attempt["error_message"] = str(error)

    max_attempts = max(1, int(cycle.get("max_attempts") or 1))
    if normalized_status == "VALIDATED":
        cycle["status"] = "VALIDATED"
        cycle["winner_attempt_id"] = attempt.get("attempt_id")
        cycle["block_reason"] = None
        cycle["alert_required"] = False
    elif normalized_status == "FAILED_PERMANENT":
        cycle["status"] = "EXHAUSTED"
        cycle["block_reason"] = attempt.get("failure_class")
        cycle["alert_required"] = True
    elif len(cycle.get("attempts", [])) >= max_attempts:
        cycle["status"] = "EXHAUSTED"
        cycle["block_reason"] = (
            "OUTCOME_UNKNOWN_LIMIT"
            if attempt.get("outcome_unknown")
            else attempt.get("failure_class") or "ATTEMPT_LIMIT"
        )
        cycle["alert_required"] = True
    else:
        cycle["status"] = "WAITING_RETRY"
        cycle["block_reason"] = None
        cycle["alert_required"] = bool(attempt.get("outcome_unknown"))

    attempt["alert_required"] = bool(cycle.get("alert_required"))

    cycle["updated_at_utc"] = now
    state["cycles"][key] = cycle
    _atomic_save(state)

    result = _copy(attempt)
    result["cycle_status"] = cycle["status"]
    result["attempts_used"] = len(cycle.get("attempts", []))
    result["max_attempts"] = max_attempts
    return result


def get_api_cycle(h1_closed_bar_time, api_stage: str) -> dict | None:
    key = _attempt_key(h1_closed_bar_time, api_stage)
    cycle = _load_state()["cycles"].get(key)
    return _copy(cycle) if isinstance(cycle, dict) else None


def get_blocking_api_attempt(h1_closed_bar_time, api_stage: str) -> dict | None:
    """Compatibility helper: only a validated/exhausted cycle is blocking."""
    cycle = get_api_cycle(h1_closed_bar_time, api_stage)
    if not cycle or cycle.get("status") not in FINAL_CYCLE_STATUSES:
        return None

    attempts = cycle.get("attempts") or []
    result = _copy(attempts[-1]) if attempts else {}
    result["cycle_status"] = cycle.get("status")
    result["winner_attempt_id"] = cycle.get("winner_attempt_id")
    return result
