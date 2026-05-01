import datetime
from zoneinfo import ZoneInfo


def test_compute_daily_schedule_at_normalizes_to_utc():
    from app.lib.utilities.job_run_helper import compute_daily_schedule_at

    scheduled = compute_daily_schedule_at(
        18,
        10,
        reference_time=datetime.datetime(
            2026, 4, 14, 18, 20, tzinfo=ZoneInfo("Asia/Shanghai")
        ),
    )

    assert scheduled == datetime.datetime(2026, 4, 14, 10, 10)


def test_compute_daily_schedule_at_uses_same_calendar_day():
    from app.lib.utilities.job_run_helper import compute_daily_schedule_at

    scheduled = compute_daily_schedule_at(
        18,
        10,
        reference_time=datetime.datetime(
            2026, 4, 14, 0, 5, tzinfo=ZoneInfo("Asia/Shanghai")
        ),
    )

    assert scheduled == datetime.datetime(2026, 4, 14, 10, 10)
