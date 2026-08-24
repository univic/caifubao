import datetime


def test_quote_status_is_classified_against_expected_date():
    from app.lib.utilities.data_asset_status_helper import classify_quote_status
    from app.model.data_asset_status import (
        STATUS_AHEAD,
        STATUS_NO_DATA,
        STATUS_OK,
        STATUS_STALE,
    )

    expected = datetime.datetime(2026, 8, 24)

    assert classify_quote_status(0, None, expected) == (
        STATUS_NO_DATA,
        "no_source_data",
    )
    assert classify_quote_status(10, datetime.datetime(2026, 8, 21), expected) == (
        STATUS_STALE,
        "behind_expected_quote_date",
    )
    assert classify_quote_status(11, expected, expected) == (STATUS_OK, None)
    assert classify_quote_status(12, datetime.datetime(2026, 8, 25), expected) == (
        STATUS_AHEAD,
        "ahead_of_expected_quote_date",
    )


def test_expected_quote_count_includes_missing_trading_days():
    from app.lib.utilities.data_asset_status_helper import expected_quote_count

    calendar = [
        datetime.datetime(2026, 8, 21),
        datetime.datetime(2026, 8, 24),
        datetime.datetime(2026, 8, 25),
    ]

    assert (
        expected_quote_count(
            1,
            datetime.datetime(2026, 8, 21),
            datetime.datetime(2026, 8, 25),
            calendar,
        )
        == 3
    )
