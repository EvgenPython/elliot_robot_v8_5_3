from datetime import datetime, timezone, timedelta


FUNDINGPIPS_TZ = timezone(
    timedelta(hours=3),
    name="FundingPips UTC+3",
)


def now_fp() -> datetime:
    """
    Текущее FundingPips Platform Time.
    """
    return datetime.now(
        timezone.utc
    ).astimezone(
        FUNDINGPIPS_TZ
    )


def get_fp_day(
    dt: datetime | None = None,
):
    """
    Текущий торговый день FundingPips.
    """

    if dt is None:
        dt = now_fp()
    else:
        if dt.tzinfo is None:
            raise ValueError(
                "datetime должен содержать timezone"
            )

        dt = dt.astimezone(
            FUNDINGPIPS_TZ
        )

    return dt.date()


def get_fp_day_start(
    dt: datetime | None = None,
) -> datetime:
    """
    Начало текущего FundingPips дня:
    00:00 UTC+3.
    """

    if dt is None:
        dt = now_fp()
    else:
        if dt.tzinfo is None:
            raise ValueError(
                "datetime должен содержать timezone"
            )

        dt = dt.astimezone(
            FUNDINGPIPS_TZ
        )

    return dt.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )


def get_next_fp_reset(
    dt: datetime | None = None,
) -> datetime:
    """
    Следующий reset Daily Loss FundingPips.
    """

    return get_fp_day_start(
        dt
    ) + timedelta(days=1)


def is_robot_working_time(
    dt: datetime | None = None,
) -> bool:
    """
    Расписание работы робота
    тоже считаем исключительно
    в FundingPips Platform Time.

    Понедельник-пятница.
    С 08:00 до 24:00 FP Time.
    """

    if dt is None:
        dt = now_fp()
    else:
        if dt.tzinfo is None:
            raise ValueError(
                "datetime должен содержать timezone"
            )

        dt = dt.astimezone(
            FUNDINGPIPS_TZ
        )

    # Суббота / воскресенье
    if dt.weekday() >= 5:
        return False

    if dt.hour < 8:
        return False

    return True


def print_time_status():
    """
    Тестовый вывод.
    """

    current = now_fp()

    print()
    print("=" * 70)
    print("FUNDINGPIPS TIME")
    print("=" * 70)

    print(
        f"Current:    "
        f"{current.isoformat()}"
    )

    print(
        f"FP day:     "
        f"{get_fp_day()}"
    )

    print(
        f"Day start:  "
        f"{get_fp_day_start().isoformat()}"
    )

    print(
        f"Next reset: "
        f"{get_next_fp_reset().isoformat()}"
    )

    print(
        f"Robot work: "
        f"{is_robot_working_time()}"
    )

    print("=" * 70)