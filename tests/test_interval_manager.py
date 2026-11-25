from datetime import datetime
from zoneinfo import ZoneInfo

from scheduler.interval_manager import get_pending_intervals


IST = ZoneInfo("Asia/Kolkata")


def test_first_pipeline_run():

    current_time = datetime(
        2026, 11, 25, 18, 37,
        tzinfo=IST
    )

    pending = get_pending_intervals(
        last_successful_timestamp=None,
        current_timestamp=current_time
    )

    assert len(pending) == 1
    assert pending[0] == datetime(
        2026, 11, 25, 18, 30,
        tzinfo=IST
    )

    print("First-run scheduling test passed.")


def test_missed_intervals():

    last_successful = datetime(
        2026, 11, 25, 18, 30,
        tzinfo=IST
    )

    current_time = datetime(
        2026, 11, 25, 19, 2,
        tzinfo=IST
    )

    pending = get_pending_intervals(
        last_successful,
        current_time
    )

    expected = [
        datetime(2026, 11, 25, 18, 45, tzinfo=IST),
        datetime(2026, 11, 25, 19, 0, tzinfo=IST)
    ]

    assert pending == expected

    print("Missed-interval recovery test passed.")


def test_no_pending_intervals():

    last_successful = datetime(
        2026, 11, 25, 18, 30,
        tzinfo=IST
    )

    current_time = datetime(
        2026, 11, 25, 18, 44,
        tzinfo=IST
    )

    pending = get_pending_intervals(
        last_successful,
        current_time
    )

    assert pending == []

    print("No-pending-interval test passed.")


def test_multiple_missed_intervals():

    last_successful = datetime(
        2026, 11, 25, 10, 0,
        tzinfo=IST
    )

    current_time = datetime(
        2026, 11, 25, 11, 0,
        tzinfo=IST
    )

    pending = get_pending_intervals(
        last_successful,
        current_time
    )

    assert len(pending) == 4

    print("Multiple missed-interval test passed.")


if __name__ == "__main__":
    test_first_pipeline_run()
    test_missed_intervals()
    test_no_pending_intervals()
    test_multiple_missed_intervals()

    print("\nAll interval manager tests passed!")