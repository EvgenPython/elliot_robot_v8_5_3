import json
from collections import Counter
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
ARCHIVE_DIR = BASE_DIR / "analysis_archive"


def _load_records() -> list[dict]:
    if not ARCHIVE_DIR.exists():
        return []

    records = []

    for path in sorted(ARCHIVE_DIR.glob("*/*.json")):
        try:
            with open(path, "r", encoding="utf-8") as file:
                record = json.load(file)
        except (OSError, json.JSONDecodeError):
            continue

        record["_path"] = str(path)
        records.append(record)

    return records


def _usage_from_record(record: dict) -> dict:
    usage = record.get("api_usage")

    if isinstance(usage, dict):
        totals = usage.get("totals")
        if isinstance(totals, dict):
            return totals
        return usage

    result = record.get("result")

    if isinstance(result, dict) and isinstance(result.get("usage"), dict):
        return result["usage"]

    return {}


def main():
    records = _load_records()

    print("=" * 80)
    print("STEP 10 — WEEKLY SCOUT / FULL AUDIT")
    print("=" * 80)

    if not records:
        print("Архив analysis_archive пока пуст.")
        return

    counts = Counter(str(record.get("cycle_type")) for record in records)

    total_input = 0
    total_output = 0
    total_cache_write = 0
    total_cache_read = 0
    total_thinking = 0
    staged_input = Counter()
    staged_output = Counter()
    staged_thinking = Counter()

    scout_no_full = 0
    scout_escalations = 0
    full_trade_actions = Counter()

    for record in records:
        cycle = str(record.get("cycle_type"))
        usage = _usage_from_record(record)

        total_input += int(usage.get("input_tokens", 0) or 0)
        total_output += int(usage.get("output_tokens", 0) or 0)
        total_cache_write += int(
            usage.get("cache_creation_input_tokens", 0) or 0
        )
        total_cache_read += int(usage.get("cache_read_input_tokens", 0) or 0)
        total_thinking += int(usage.get("thinking_tokens", 0) or 0)

        staged_usage = record.get("api_usage")
        if isinstance(staged_usage, dict):
            for stage_name in ("market_map", "trade_decision"):
                stage_usage = staged_usage.get(stage_name)
                if not isinstance(stage_usage, dict):
                    continue
                staged_input[stage_name] += int(
                    stage_usage.get("input_tokens", 0) or 0
                )
                staged_output[stage_name] += int(
                    stage_usage.get("output_tokens", 0) or 0
                )
                staged_thinking[stage_name] += int(
                    stage_usage.get("thinking_tokens", 0) or 0
                )

        if cycle == "SCOUT":
            result = record.get("result") or {}
            if result.get("full_analysis_required"):
                scout_escalations += 1
            else:
                scout_no_full += 1

        if cycle.startswith("FULL"):
            result = record.get("result") or {}
            recommendation = result.get("recommendation") or {}
            action = recommendation.get("action")
            if action:
                full_trade_actions[str(action)] += 1

    print("Cycles:")
    for key, value in sorted(counts.items()):
        print(f"  {key:24s} {value}")

    print()
    print(f"Scout NO FULL:           {scout_no_full}")
    print(f"Scout escalations:       {scout_escalations}")

    print()
    print("FULL actions:")
    if full_trade_actions:
        for key, value in sorted(full_trade_actions.items()):
            print(f"  {key:24s} {value}")
    else:
        print("  Нет сохранённых FULL actions.")

    print()
    print("API tokens from archive:")
    print(f"  Input:                 {total_input:,}")
    print(f"  Output:                {total_output:,}")
    print(f"  Thinking detail:       {total_thinking:,}")
    print(f"  Cache write:           {total_cache_write:,}")
    print(f"  Cache read:            {total_cache_read:,}")

    if staged_input or staged_output:
        print()
        print("Staged FULL split:")
        for stage_name in ("market_map", "trade_decision"):
            print(
                f"  {stage_name:18s} "
                f"input={staged_input[stage_name]:,}; "
                f"output={staged_output[stage_name]:,}; "
                f"thinking={staged_thinking[stage_name]:,}"
            )

    print()
    print("Для оценки качества Scout сравни SCOUT_NO_FULL с ближайшим следующим")
    print("FULL_ESCALATED/FULL_FALLBACK или дневным FULL_SCHEDULED следующего дня.")
    print("Архив хранит payload/result и точный тип каждого analysis cycle.")
    print("=" * 80)


if __name__ == "__main__":
    main()
