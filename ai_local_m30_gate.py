from __future__ import annotations

from datetime import timedelta

from ai_local_event_gate import (
    _bar_value,
    _collect_price_levels,
    _float,
    _parse_time,
    _rows,
    _watch_owned,
)


def _result(
    *,
    snapshot: dict,
    possible_setup: bool,
    trigger_kind: str,
    reason: str,
    details: list[str] | None = None,
    structural_warning: bool = False,
) -> dict:

    return {
        "instrument": str(
            snapshot.get("instrument")
            or "XAUUSD"
        ),
        "timestamp": str(
            snapshot.get("generated_at_fp")
            or ""
        ),
        "possible_setup": bool(
            possible_setup
        ),
        "material_change": bool(
            possible_setup
            or structural_warning
        ),
        "structural_warning": bool(
            structural_warning
        ),
        "trigger_kind": str(
            trigger_kind
        ),
        "details": list(
            details or []
        ),
        "reason": str(
            reason
        ),
        "local_cost_gate": True,
        "usage": {
            "input_tokens": 0,
            "output_tokens": 0,
        },
        "request_id": None,
    }


def _latest_h1_time(
    snapshot: dict,
):

    rows = _rows(
        snapshot,
        "H1",
        1,
    )

    if not rows:
        return None

    bar = rows[-1]

    return _parse_time(
        _bar_value(bar, "time")
        or _bar_value(bar, "time_fp")
        or _bar_value(bar, "datetime")
    )


def _same_reference_h1(
    snapshot: dict,
    previous_reference: dict,
) -> bool:

    current = _latest_h1_time(
        snapshot
    )

    previous = _parse_time(
        previous_reference.get(
            "h1_closed_bar_time_fp"
        )
    )

    if (
        current is None
        or previous is None
    ):
        return False

    try:
        return current == previous
    except Exception:
        return False


def inspect_m30_event(
    snapshot: dict,
    previous_reference: dict,
) -> dict:
    """
    FREE deterministic M30 gate.

    It does NOT choose direction.
    It does NOT create Entry/SL/TP.
    It only decides whether buying one M30 Claude decision
    is justified.
    """

    analysis = (
        previous_reference.get("analysis")
        if isinstance(
            previous_reference,
            dict,
        )
        else None
    )


    if not isinstance(
        analysis,
        dict,
    ):

        return _result(
            snapshot=snapshot,
            possible_setup=False,
            trigger_kind="no_reference",
            reason=(
                "No validated daily FULL reference; "
                "M30 entry decision is blocked."
            ),
            structural_warning=True,
        )


    # FULL already analysed exactly this H1 snapshot.
    # Do not immediately buy another M30 decision.
    if _same_reference_h1(
        snapshot,
        previous_reference,
    ):

        return _result(
            snapshot=snapshot,
            possible_setup=False,
            trigger_kind="same_full_snapshot",
            reason=(
                "Current H1 is already owned by the latest FULL; "
                "duplicate M30 decision is unnecessary."
            ),
        )


    # Existing conditional plan is already watched by M15/M5 ENTRY_CHECK.
    if _watch_owned(
        previous_reference
    ):

        return _result(
            snapshot=snapshot,
            possible_setup=False,
            trigger_kind="entry_watch",
            reason=(
                "Existing Entry Watch already owns the M15/M5 trigger."
            ),
        )


    bars = _rows(
        snapshot,
        "M30",
        2,
    )


    if not bars:

        return _result(
            snapshot=snapshot,
            possible_setup=True,
            trigger_kind="uncertainty",
            reason=(
                "Latest closed M30 cannot be read locally; "
                "one bounded M30 decision is justified."
            ),
        )


    current = bars[-1]


    high = _float(
        _bar_value(
            current,
            "high",
        )
    )

    low = _float(
        _bar_value(
            current,
            "low",
        )
    )

    close = _float(
        _bar_value(
            current,
            "close",
        )
    )


    if (
        high is None
        or low is None
        or close is None
        or close <= 0
    ):

        return _result(
            snapshot=snapshot,
            possible_setup=True,
            trigger_kind="uncertainty",
            reason=(
                "Closed M30 OHLC is incomplete; "
                "bounded M30 analysis is justified."
            ),
        )


    # --------------------------------------------------------
    # Higher-map invalidation warning.
    # M30 may not independently rewrite H1 structure.
    # --------------------------------------------------------

    wave_count = (
        analysis.get(
            "wave_count"
        )
        or {}
    )


    invalidation = _float(
        wave_count.get(
            "invalidation_level"
        )
    )

    direction = str(
        wave_count.get(
            "direction"
        )
        or ""
    ).lower()


    if invalidation is not None:

        broken = False

        if (
            direction
            in {
                "bullish",
                "up",
                "long",
            }
            and close
            < invalidation
        ):
            broken = True

        elif (
            direction
            in {
                "bearish",
                "down",
                "short",
            }
            and close
            > invalidation
        ):
            broken = True


        if broken:

            return _result(
                snapshot=snapshot,
                possible_setup=False,
                trigger_kind="structure_warning",
                structural_warning=True,
                details=[
                    f"M30 close crossed HTF invalidation {invalidation}"
                ],
                reason=(
                    "M30 crossed the validated higher-timeframe "
                    "invalidation. Wait for the H1 structural cycle; "
                    "do not create an M30 entry from a stale map."
                ),
            )


    # --------------------------------------------------------
    # Validated analytical levels only.
    # Visualization is deliberately excluded.
    # --------------------------------------------------------

    raw_levels = _collect_price_levels(
        {
            "recommendation": (
                analysis.get(
                    "recommendation"
                )
            ),
            "wave_count": (
                analysis.get(
                    "wave_count"
                )
            ),
        }
    )


    levels = []

    for value in raw_levels:

        if (
            abs(value - close)
            / close
            > 0.04
        ):
            continue

        if not any(
            abs(value - old)
            <= max(
                close * 0.00002,
                0.01,
            )
            for old in levels
        ):
            levels.append(
                value
            )


    touched = [
        value
        for value in levels
        if low <= value <= high
    ]


    near = [
        value
        for value in levels
        if (
            abs(close - value)
            / close
            <= 0.0015
        )
    ]


    # --------------------------------------------------------
    # M30 movement / breakout.
    # More permissive than the H1 gate by design.
    # --------------------------------------------------------

    range_fraction = (
        max(
            0.0,
            high - low,
        )
        / close
    )


    previous_close = None
    previous_high = None
    previous_low = None


    if len(bars) >= 2:

        previous_close = _float(
            _bar_value(
                bars[-2],
                "close",
            )
        )

        previous_high = _float(
            _bar_value(
                bars[-2],
                "high",
            )
        )

        previous_low = _float(
            _bar_value(
                bars[-2],
                "low",
            )
        )


    close_change = 0.0


    if (
        previous_close is not None
        and previous_close > 0
    ):

        close_change = (
            abs(
                close - previous_close
            )
            / previous_close
        )


    strong_move = bool(
        range_fraction >= 0.0025
        or close_change >= 0.0015
    )


    breakout = False


    if (
        previous_high is not None
        and previous_low is not None
    ):

        breakout = bool(
            close
            > previous_high * 1.0002
            or close
            < previous_low * 0.9998
        )


    if (
        touched
        or near
        or strong_move
        or breakout
    ):

        details = []


        if touched:

            details.append(
                "touched levels: "
                + ", ".join(
                    f"{x:.5f}"
                    for x in touched[:6]
                )
            )


        if near:

            details.append(
                "near levels: "
                + ", ".join(
                    f"{x:.5f}"
                    for x in near[:6]
                )
            )


        if strong_move:

            details.append(
                (
                    "M30 movement "
                    f"range={range_fraction:.4%}; "
                    f"close_change={close_change:.4%}"
                )
            )


        if breakout:

            details.append(
                "closed M30 broke the previous M30 range"
            )


        return _result(
            snapshot=snapshot,
            possible_setup=True,
            trigger_kind="m30_setup",
            details=details,
            reason=(
                "Closed M30 produced a decision-relevant "
                "movement, breakout or level interaction."
            ),
        )


    # One deliberately bounded insurance check during the active day.
    time_value = (
        _bar_value(
            current,
            "time",
        )
        or _bar_value(
            current,
            "time_fp",
        )
        or _bar_value(
            current,
            "datetime",
        )
    )


    bar_time = _parse_time(
        time_value
    )


    if bar_time is not None:

        close_time = (
            bar_time
            + timedelta(
                minutes=30
            )
        )

        if (
            close_time.hour == 14
            and close_time.minute == 30
        ):

            return _result(
                snapshot=snapshot,
                possible_setup=True,
                trigger_kind="insurance",
                details=[
                    "single 14:30 FP M30 insurance decision"
                ],
                reason=(
                    "No strong local event, but this is the "
                    "single bounded M30 insurance check."
                ),
            )


    return _result(
        snapshot=snapshot,
        possible_setup=False,
        trigger_kind="none",
        reason=(
            "Closed M30 remains inside the validated context "
            "without a material setup event."
        ),
    )
