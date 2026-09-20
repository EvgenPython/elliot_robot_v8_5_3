"""Standard-price estimates, not invoices. Thinking is part of output tokens."""
from __future__ import annotations

PRICE_DATE = "2026-09-15"
SOURCE = "https://platform.claude.com/docs/en/about-claude/pricing"
RATES = {
    "claude-sonnet-5": (2.0, 10.0, 0.20, 2.50),
    "claude-haiku-4-5": (1.0, 5.0, 0.10, 1.25),
    "claude-sonnet-4-6": (3.0, 15.0, 0.30, 3.75),
    "claude-sonnet-4-5": (3.0, 15.0, 0.30, 3.75),
}


def estimate_cost(model, usage):
    rates = next((rate for name, rate in RATES.items()
                  if str(model) == name or str(model).startswith(name + "-")), None)
    if rates is None or not isinstance(usage, dict) or not {"input_tokens", "output_tokens"} <= usage.keys():
        return {"usd": None, "reason": "unknown_model_or_usage"}
    try:
        counts = [int(usage.get(key, 0) or 0) for key in (
            "input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens")]
        if any(value < 0 for value in counts):
            raise ValueError("negative usage")
        # Cache writes may use a different TTL. Production has caching off.
        if counts[3]:
            return {"usd": None, "reason": "cache_write_ttl_not_recorded"}
        return {"usd": round(sum(n * r for n, r in zip(counts, rates)) / 1_000_000, 8),
                "price_date": PRICE_DATE, "source": SOURCE, "basis": "standard_rates_estimate"}
    except (TypeError, ValueError, OverflowError):
        return {"usd": None, "reason": "invalid_usage"}
