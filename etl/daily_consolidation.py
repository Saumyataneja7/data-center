from pathlib import Path
from datetime import date

import duckdb

import argparse
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "telemetry"

DAILY_DATA_DIR = PROJECT_ROOT / "data" / "daily"

EXPECTED_SERVERS = 500
EXPECTED_INTERVALS = 96
EXPECTED_DAILY_RECORDS = (
    EXPECTED_SERVERS * EXPECTED_INTERVALS
)
IST = ZoneInfo("Asia/Kolkata")


def get_default_target_date() -> date:
    """
    Return yesterday's date in Indian Standard Time.

    Daily consolidation should process a completed day,
    not the current incomplete day.
    """
    now_ist = datetime.now(IST)
    return (now_ist.date() - timedelta(days=1))


def parse_target_date(date_string: str) -> date:
    """
    Convert a YYYY-MM-DD string into a Python date.
    """
    try:
        return date.fromisoformat(date_string)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "Date must be in YYYY-MM-DD format."
        ) from error

def get_expected_batch_names():
    return {
        f"batch={hour:02d}{minute:02d}.parquet"
        for hour in range(24)
        for minute in (0, 15, 30, 45)
    }


def validate_batches(batch_files):

    if len(batch_files) != EXPECTED_INTERVALS:
        raise ValueError(
            f"Expected {EXPECTED_INTERVALS} batches, "
            f"found {len(batch_files)}."
        )

    connection = duckdb.connect(database=":memory:")

    try:
        for batch_file in batch_files:

            path = str(batch_file).replace("'", "''")

            result = connection.execute(f"""
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT server_id),
                    COUNT(DISTINCT timestamp)
                FROM read_parquet('{path}')
            """).fetchone()

            records, servers, timestamps = result

            if records != EXPECTED_SERVERS:
                raise ValueError(
                    f"{batch_file.name}: expected 500 records, "
                    f"found {records}."
                )

            if servers != EXPECTED_SERVERS:
                raise ValueError(
                    f"{batch_file.name}: expected 500 unique servers, "
                    f"found {servers}."
                )

            if timestamps != 1:
                raise ValueError(
                    f"{batch_file.name}: inconsistent timestamps."
                )

    finally:
        connection.close()

def validate_daily_timestamps(batch_files, target_date: date) -> None:
    """
    Verify that the 96 batches contain exactly the expected
    15-minute timestamps for the target date in IST.
    """

    expected_timestamps = {
        datetime.combine(
            target_date,
            time(hour=hour, minute=minute)
        )
        for hour in range(24)
        for minute in (0, 15, 30, 45)
    }

    actual_timestamps = set()

    for batch_file in batch_files:
        dataframe = pd.read_parquet(
            batch_file,
            columns=["timestamp"]
        )

        timestamps = pd.to_datetime(dataframe["timestamp"]).drop_duplicates()

        if len(timestamps) != 1:
            raise ValueError(
                f"{batch_file.name} must contain exactly one timestamp."
            )

        timestamp = timestamps.iloc[0]

        # Convert timezone-aware timestamps into local IST,
        # then remove timezone information for comparison.
        if timestamp.tzinfo is not None:
            timestamp = (
                timestamp
                .tz_convert(IST)
                .tz_localize(None)
            )

        actual_timestamps.add(timestamp.to_pydatetime())

    if actual_timestamps != expected_timestamps:
        missing = sorted(expected_timestamps - actual_timestamps)
        unexpected = sorted(actual_timestamps - expected_timestamps)

        raise ValueError(
            "Daily timestamp validation failed. "
            f"Missing timestamps: {missing}. "
            f"Unexpected timestamps: {unexpected}."
        )

    print("All 96 expected IST timestamps are present.")

def consolidate_day(target_date: date):

    date_folder = RAW_DATA_DIR / f"date={target_date}"

    if not date_folder.exists():
        raise FileNotFoundError(
            f"No telemetry directory found for {target_date}."
        )

    expected_names = get_expected_batch_names()

    batch_files = sorted(date_folder.glob("batch=*.parquet"))

    actual_names = {file.name for file in batch_files}

    missing = expected_names - actual_names
    unexpected = actual_names - expected_names

    if missing:
        raise ValueError(
            f"Daily consolidation incomplete. "
            f"Missing {len(missing)} batches: "
            f"{sorted(missing)}"
        )

    if unexpected:
        raise ValueError(
            f"Unexpected batch files found: {sorted(unexpected)}"
        )

    print(f"Found {len(batch_files)} batches for {target_date}.")

    validate_batches(batch_files)
    validate_daily_timestamps(batch_files, target_date)

    print("All individual batches passed validation.")

    DAILY_DATA_DIR.mkdir(parents=True, exist_ok=True)

    output_file = (
        DAILY_DATA_DIR /
        f"telemetry_{target_date}.parquet"
    )

    temporary_file = output_file.with_name(
        output_file.stem + ".tmp.parquet"
    )

    if output_file.exists():
        raise FileExistsError(
            f"Daily output already exists: {output_file}"
        )

    connection = duckdb.connect(database=":memory:")

    try:
        paths = [
            "'" + str(file).replace("'", "''") + "'"
            for file in batch_files
        ]

        path_list = ", ".join(paths)

        output_path = str(temporary_file).replace("'", "''")

        connection.execute(f"""
            COPY (
                SELECT *
                FROM read_parquet([{path_list}])
                ORDER BY timestamp, server_id
            )
            TO '{output_path}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        result = connection.execute(f"""
            SELECT
                COUNT(*),
                COUNT(DISTINCT server_id || '|' || CAST(timestamp AS VARCHAR)),
                COUNT(DISTINCT timestamp)
            FROM read_parquet('{output_path}')
        """).fetchone()

        total_records, unique_readings, unique_timestamps = result

        if total_records != EXPECTED_DAILY_RECORDS:
            raise ValueError(
                f"Expected {EXPECTED_DAILY_RECORDS} records, "
                f"found {total_records}."
            )

        if unique_readings != EXPECTED_DAILY_RECORDS:
            raise ValueError("Duplicate server readings detected.")

        if unique_timestamps != EXPECTED_INTERVALS:
            raise ValueError(
                f"Expected 96 timestamps, found {unique_timestamps}."
            )

        temporary_file.replace(output_file)

        print("\nDaily consolidation successful.")
        print(f"Date: {target_date}")
        print(f"Total records: {total_records}")
        print(f"Unique readings: {unique_readings}")
        print(f"Unique timestamps: {unique_timestamps}")
        print(f"Output: {output_file}")

        return output_file

    except Exception:
        if temporary_file.exists():
            temporary_file.unlink()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Consolidate a completed day of data center telemetry."
    )

    parser.add_argument(
        "--date",
        type=parse_target_date,
        help="Date to consolidate in YYYY-MM-DD format. "
             "Defaults to yesterday in IST."
    )

    args = parser.parse_args()

    target_date = args.date or get_default_target_date()

    print(f"Target consolidation date: {target_date}")

    consolidate_day(target_date)