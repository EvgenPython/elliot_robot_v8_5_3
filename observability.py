"""Development audit: UTF-8 JSONL, immutable API artifacts, no trade authority.

Logs are per process/day and split at 20 MiB. Nothing is silently deleted.
Diagnostic I/O errors cannot interrupt analysis or duplicate an order.
"""
from __future__ import annotations

import contextlib
import contextvars
import functools
import hashlib
import inspect
import json
import math
import os
import re
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

VERSION = "8.5.3"
BASE_DIR = Path(__file__).resolve().parent
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"_{os.getpid()}_" + uuid.uuid4().hex[:8]
_CONTEXT = contextvars.ContextVar("robot_audit_context", default={})
_LOCK = threading.RLock()
_PARTS = {}
_LAST = {}
_WARNED = False
_SECRET = re.compile(r"api.?key|password|secret|authorization|access.?token|refresh.?token|ingest.?token|webhook.?token|^token$", re.I)
_TOKEN = re.compile(r"sk-ant-[A-Za-z0-9_-]+|(?i:Bearer)\s+[A-Za-z0-9_.\-]+")


def safe(value):
    if isinstance(value, dict):
        return {str(k): "[REDACTED]" if _SECRET.search(str(k)) else safe(v) for k, v in value.items()}
    # MT5 structures are namedtuples.  Preserve their field names in the
    # audit trail; converting them to a plain list makes later diagnosis of
    # broker replies unnecessarily ambiguous.
    if hasattr(value, "_asdict"):
        return safe(value._asdict())
    if isinstance(value, (list, tuple, set)):
        return [safe(item) for item in value]
    if hasattr(value, "to_dict"):
        try:
            return safe(value.to_dict(orient="records"))
        except TypeError:
            return safe(value.to_dict())
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if callable(value):
        return getattr(value, "__name__", "callback")
    return _TOKEN.sub("[REDACTED]", str(value))


def _warn(error):
    global _WARNED
    if not _WARNED:
        _WARNED = True
        try:
            sys.__stderr__.write(f"[DIAGNOSTICS WARNING] Log write failed: {type(error).__name__}\n")
        except Exception:
            pass


def _root():
    return Path(os.getenv("ROBOT_DIAGNOSTICS_DIR", str(BASE_DIR / "logs" / "events")))


def emit(component, event, *, data=None, level="INFO", repeat_key=None, repeat_seconds=60):
    """Return written path or None; throttle only explicitly repetitive events."""
    try:
        with _LOCK:
            clean = safe(data)
            if repeat_key:
                key = (component, event, str(repeat_key))
                fingerprint = hashlib.sha256(json.dumps(clean, sort_keys=True).encode()).hexdigest()
                previous = _LAST.get(key)
                stamp = time.monotonic()
                if previous and previous[0] == fingerprint and stamp - previous[1] < repeat_seconds:
                    return None
                _LAST[key] = (fingerprint, stamp)
            now = datetime.now(timezone.utc)
            record = {"schema_version": 1, "timestamp_utc": now.isoformat(), "version": VERSION,
                      "run_id": RUN_ID, "pid": os.getpid(), **safe(_CONTEXT.get()),
                      "component": component, "event": event, "level": level, "data": clean}
            line = json.dumps(record, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n"
            folder = _root() / now.strftime("%Y-%m-%d")
            folder.mkdir(parents=True, exist_ok=True)
            name = re.sub(r"[^A-Za-z0-9_-]", "_", component)
            key = (str(folder), name)
            part = _PARTS.get(key, 0)
            path = folder / f"{name}_{RUN_ID}_{part:03}.jsonl"
            if path.exists() and path.stat().st_size + len(line.encode("utf-8")) > 20 * 1024 * 1024:
                part += 1
                _PARTS[key] = part
                path = folder / f"{name}_{RUN_ID}_{part:03}.jsonl"
            with path.open("a", encoding="utf-8") as file:
                file.write(line)
            return path
    except Exception as error:
        _warn(error)
        return None


@contextlib.contextmanager
def context(**values):
    token = _CONTEXT.set({**_CONTEXT.get(), **values})
    try:
        yield
    finally:
        _CONTEXT.reset(token)


def artifact(name, data):
    """Immutable sanitized request/response snapshot; failures are non-fatal."""
    try:
        folder = _root().parent / "api" / datetime.now(timezone.utc).strftime("%Y-%m-%d")
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / (re.sub(r"[^A-Za-z0-9_-]", "_", name) + "_" + uuid.uuid4().hex + ".json")
        path.write_text(json.dumps(safe(data), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
        return str(path)
    except Exception as error:
        _warn(error)
        return None


def observe(component, *, inputs=False, artifacts=False):
    def decorate(function):
        signature = inspect.signature(function)
        @functools.wraps(function)
        def wrapped(*args, **kwargs):
            call_id = uuid.uuid4().hex
            started = time.monotonic()
            values = dict(signature.bind(*args, **kwargs).arguments)
            with context(call_id=call_id):
                request_path = artifact(function.__name__ + "_request", values) if artifacts else None
                emit(component, function.__name__ + ".started", data={
                    "request_artifact": request_path, "inputs": values if inputs and not artifacts else None})
                try:
                    result = function(*args, **kwargs)
                except BaseException as error:
                    emit(component, function.__name__ + ".failed", level="ERROR", data={
                        "duration_seconds": round(time.monotonic() - started, 3),
                        "error_type": type(error).__name__, "error": str(error),
                        "traceback": traceback.format_exc()})
                    raise
                response_path = artifact(function.__name__ + "_response", result) if artifacts else None
                emit(component, function.__name__ + ".completed", data={
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "response_artifact": response_path, "result": result if not artifacts else None})
                return result
        return wrapped
    return decorate


def manifest():
    """Versions and content hashes allow diagnosis without leaking configs."""
    files = {}
    for path in sorted(BASE_DIR.glob("*.py")) + sorted((BASE_DIR / "config").glob("*.json")):
        files[str(path.relative_to(BASE_DIR))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {"version": VERSION, "run_id": RUN_ID, "python": sys.version, "root": str(BASE_DIR),
            "file_sha256": files}


def start_session(role):
    try:
        emit("session", "started", data={"role": role, **manifest()})
    except Exception as error:
        _warn(error)
