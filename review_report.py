"""Offline archive report. No MT5 connection, no Claude calls."""
from __future__ import annotations
import argparse
import json
from collections import Counter
from pathlib import Path
from api_costs import estimate_cost


def attempts(value):
    if isinstance(value, dict):
        if value.get("attempt_id") and value.get("api_stage"):
            yield value
        for item in value.values():
            yield from attempts(item)
    elif isinstance(value, list):
        for item in value:
            yield from attempts(item)


def build_report(root, start="0000-00-00", end="9999-99-99"):
    unique, cycles, errors = {}, {}, []
    for path in sorted(Path(root).rglob("*.json")):
        # Only analysis archives, never authentication/config files.
        if "analysis_archive" not in path.parts:
            continue
        try:
            record = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(record, dict):
                continue
            date = str(record.get("snapshot_time_fp") or (record.get("payload") or {}).get("timestamp") or "")[:10]
            if not start <= date <= end:
                continue
            key = record.get("event_id") or record.get("cycle_id") or (date, path.name)
            old = cycles.get(key)
            if old is None or record.get("revision", 0) >= old.get("revision", 0):
                cycles[key] = record
            for attempt in attempts(record):
                key = attempt["attempt_id"]
                old = unique.get(key, {})
                # Most complete/final record wins over repeated embedded copies.
                if (bool(attempt.get("usage")), str(attempt.get("updated_at_fp", ""))) >= (bool(old.get("usage")), str(old.get("updated_at_fp", ""))):
                    unique[key] = attempt
        except (OSError, ValueError, TypeError) as error:
            errors.append({"file": str(path), "error": str(error)})
    usage_by_model, total, unknown = {}, 0.0, 0
    for item in unique.values():
        diagnostics = item.get("diagnostics") or {}
        model = item.get("model") or diagnostics.get("model") or "unknown"
        usage = item.get("usage") or diagnostics.get("usage")
        cost = estimate_cost(model, usage)
        group = usage_by_model.setdefault(model, {"calls": 0, "known_cost_usd": 0.0, "input_tokens": 0, "output_tokens": 0, "unknown_cost_calls": 0})
        group["calls"] += 1
        if isinstance(usage, dict):
            for key in ("input_tokens", "output_tokens"):
                group[key] += int(usage.get(key, 0) or 0)
        if cost["usd"] is None:
            unknown += 1
            group["unknown_cost_calls"] += 1
        else:
            total += cost["usd"]
            group["known_cost_usd"] = round(group["known_cost_usd"] + cost["usd"], 8)
    failures = []
    funnel_rows = []
    for record in cycles.values():
        messages = {key: val for key, val in record.items() if (key.endswith("validation_error") or key == "error") and val}
        if messages:
            failures.append({"at": record.get("snapshot_time_fp"), "cycle_type": record.get("cycle_type"), "errors": messages})
        result = record.get("result") or {}
        recommendation = result.get("recommendation") or {}
        risk = record.get("risk_report") or {}
        state = record.get("trade_state_result") or {}
        execution = record.get("execution_report") or {}
        if recommendation or risk or execution:
            funnel_rows.append({
                "at": record.get("snapshot_time_fp"),
                "h1": record.get("h1_closed_bar_time_fp"),
                "cycle_type": record.get("cycle_type"),
                "claude_action": recommendation.get("action"),
                "setup_type": recommendation.get("setup_type"),
                "confidence": recommendation.get("confidence"),
                "risk_decision": risk.get("decision"),
                "risk_reasons": list(risk.get("reasons") or []),
                "trade_state_action": state.get("state_action"),
                "execution_decision": execution.get("decision"),
                "order_send_called": bool(execution.get("order_send_called")),
            })
    funnel_rows.sort(key=lambda item: str(item.get("at") or ""))
    blocker_counts = Counter()
    for row in funnel_rows:
        for reason in row["risk_reasons"]:
            blocker_counts[str(reason)] += 1
    return {"cycles": len(cycles), "cycles_by_type": dict(Counter(r.get("cycle_type", "unknown") for r in cycles.values())),
            "unique_api_attempts": len(unique), "models": usage_by_model,
            "estimated_known_cost_usd": round(total, 8), "unknown_cost_attempts": unknown,
            "validation_failures": failures, "read_errors": errors,
            "trade_funnel": {
                "evaluated_decisions": len(funnel_rows),
                "claude_actions": dict(Counter(str(row.get("claude_action") or "unknown") for row in funnel_rows)),
                "risk_decisions": dict(Counter(str(row.get("risk_decision") or "unknown") for row in funnel_rows)),
                "execution_decisions": dict(Counter(str(row.get("execution_decision") or "unknown") for row in funnel_rows)),
                "order_send_calls": sum(row["order_send_called"] for row in funnel_rows),
                "blockers": dict(blocker_counts.most_common()),
                "latest": funnel_rows[-50:],
            },
            "note": "Standard-price estimate; duplicated archives/attempts deduplicated. Not an invoice or a profitability backtest."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--from", dest="start", default="0000-00-00")
    parser.add_argument("--to", dest="end", default="9999-99-99")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    text = json.dumps(build_report(args.root, args.start, args.end), ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
