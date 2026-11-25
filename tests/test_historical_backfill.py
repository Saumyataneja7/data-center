import io
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from telemetry_generator.pipeline_runner import PipelineRunner


IST = ZoneInfo("Asia/Kolkata")


def test_three_day_historical_backfill():

    print("\nTEST: Three-day historical backfill")

    # Last successfully committed interval.
    last_successful = datetime(
        2026, 11, 25, 0, 0,
        tzinfo=IST
    )

    # Pipeline restarts three days later.
    current_time = datetime(
        2026, 11, 28, 0, 7,
        tzinfo=IST
    )

    # Create a runner without initializing its real components.
    runner = PipelineRunner.__new__(PipelineRunner)

    # Track which intervals the pipeline attempts to process.
    processed_intervals = []

    runner.get_last_successful_timestamp = (
        lambda: last_successful
    )

    runner.run_once = (
        lambda timestamp: processed_intervals.append(timestamp)
    )

    # Suppress the pipeline's progress output during this test.
    with redirect_stdout(io.StringIO()):

        processed_count = runner.run_pending_intervals(
            current_timestamp=current_time
        )

    # Construct the expected sequence independently.
    expected_intervals = []

    timestamp = last_successful + timedelta(minutes=15)

    latest_due = datetime(
        2026, 11, 28, 0, 0,
        tzinfo=IST
    )

    while timestamp <= latest_due:

        expected_intervals.append(timestamp)

        timestamp += timedelta(minutes=15)

    # Verify total interval count.
    assert processed_count == 288
    assert len(processed_intervals) == 288

    # Verify chronological processing.
    assert processed_intervals == expected_intervals

    # Verify no duplicate timestamps.
    assert len(set(processed_intervals)) == 288

    # Verify first and last intervals.
    assert processed_intervals[0] == datetime(
        2026, 11, 25, 0, 15,
        tzinfo=IST
    )

    assert processed_intervals[-1] == datetime(
        2026, 11, 28, 0, 0,
        tzinfo=IST
    )

    print("Total missed intervals: 288")
    print("Chronological ordering verified.")
    print("Duplicate intervals: 0")
    print("First interval: 2026-11-25 00:15 IST")
    print("Last interval: 2026-11-28 00:00 IST")

    print("\nThree-day historical backfill test passed!")


if __name__ == "__main__":

    test_three_day_historical_backfill()

    print("\nALL HISTORICAL BACKFILL TESTS PASSED!")