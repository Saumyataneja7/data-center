from datetime import datetime, timedelta


def validate_timestamp(timestamp):
    """Ensure the timestamp includes timezone information."""

    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("Timestamp must be timezone-aware.")


def floor_to_interval(timestamp, interval_minutes=15):
    """
    Round a timestamp down to the latest 15-minute boundary.

    Example:
    18:37 becomes 18:30.
    """

    validate_timestamp(timestamp)

    if interval_minutes <= 0 or 60 % interval_minutes != 0:
        raise ValueError(
            "Interval must be a positive divisor of 60."
        )

    aligned_minute = (
        timestamp.minute // interval_minutes
    ) * interval_minutes

    return timestamp.replace(
        minute=aligned_minute,
        second=0,
        microsecond=0
    )


def get_pending_intervals(
    last_successful_timestamp,
    current_timestamp,
    interval_minutes=15
):
    """
    Return all intervals that need processing.

    If no previous checkpoint exists, return the latest
    aligned interval.

    Otherwise, return every interval after the checkpoint
    up to the latest aligned interval.
    """

    validate_timestamp(current_timestamp)

    latest_due = floor_to_interval(
        current_timestamp,
        interval_minutes
    )

    if last_successful_timestamp is None:
        return [latest_due]

    validate_timestamp(last_successful_timestamp)

    # Convert the checkpoint into the current timezone.
    last_successful_timestamp = (
        last_successful_timestamp.astimezone(
            current_timestamp.tzinfo
        )
    )

    # Check that the checkpoint is interval-aligned.
    aligned_checkpoint = floor_to_interval(
        last_successful_timestamp,
        interval_minutes
    )

    if aligned_checkpoint != last_successful_timestamp:
        raise ValueError(
            "Checkpoint timestamp is not interval-aligned."
        )

    pending = []

    next_timestamp = last_successful_timestamp + timedelta(
        minutes=interval_minutes
    )

    while next_timestamp <= latest_due:
        pending.append(next_timestamp)

        next_timestamp += timedelta(
            minutes=interval_minutes
        )

    return pending