from pathlib import Path
from datetime import date

import duckdb

import argparse
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
import pandas as pd

import json

from telemetry_generator.config import CHECKPOINT_FILE
from scheduler.process_lock import exclusive_process_lock

from utils.logger import get_logger


logger = get_logger(
    "daily_consolidation",
    "daily_consolidation_structured.log"
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "telemetry"

DAILY_DATA_DIR = PROJECT_ROOT / "data" / "daily"

EXPECTED_SERVERS = 500
EXPECTED_INTERVALS = 96
EXPECTED_DAILY_RECORDS = (
    EXPECTED_SERVERS * EXPECTED_INTERVALS
)
IST = ZoneInfo("Asia/Kolkata")

def create_checkpoint(checkpoint_file, timestamp):

    checkpoint_file.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    checkpoint_file.write_text(
        json.dumps({
            "last_successful_timestamp": timestamp,
            "status": "SUCCESS"
        })
    )

def get_default_target_date() -> date:
    """
    Return yesterday's date in Indian Standard Time.

    Daily consolidation should process a completed day,
    not the current incomplete day.
    """
    now_ist = datetime.now(IST)
    return (now_ist.date() - timedelta(days=1))


def get_last_completed_date(checkpoint_file=None):
    """
    Determine the latest fully completed telemetry date
    using the telemetry pipeline checkpoint.

    A date is complete only when its 23:45 IST interval
    has been successfully committed.
    """

    if checkpoint_file is None:
        checkpoint_file = CHECKPOINT_FILE

    checkpoint_file = Path(checkpoint_file)

    if not checkpoint_file.exists():
        logger.warning("No telemetry checkpoint found.")
        return None

    with open(checkpoint_file, "r") as file:
        checkpoint = json.load(file)

    timestamp = datetime.fromisoformat(
        checkpoint["last_successful_timestamp"]
    )

    if timestamp.tzinfo is None:
        raise ValueError(
            "Telemetry checkpoint must be timezone-aware."
        )

    timestamp = timestamp.astimezone(IST)

    final_interval = time(23, 45)

    if timestamp.time() >= final_interval:
        return timestamp.date()

    return timestamp.date() - timedelta(days=1)


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

    logger.info("All 96 expected IST timestamps are present.")


def validate_daily_output(output_file: Path, target_date: date) -> None:
    """
    Validate an existing daily Parquet file before skipping it
    during historical consolidation recovery.
    """

    if not output_file.exists():
        raise FileNotFoundError(output_file)

    connection = duckdb.connect(database=":memory:")

    try:
        result = connection.execute(f"""
            SELECT
                COUNT(*),
                COUNT(DISTINCT server_id || '|' || CAST(timestamp AS VARCHAR)),
                COUNT(DISTINCT timestamp)
            FROM read_parquet('{str(output_file).replace("'", "''")}')
        """).fetchone()

        records, unique_readings, unique_timestamps = result

        if records != EXPECTED_DAILY_RECORDS:
            raise ValueError(
                f"{output_file.name}: expected "
                f"{EXPECTED_DAILY_RECORDS} records, found {records}."
            )

        if unique_readings != EXPECTED_DAILY_RECORDS:
            raise ValueError(
                f"{output_file.name}: duplicate readings detected."
            )

        if unique_timestamps != EXPECTED_INTERVALS:
            raise ValueError(
                f"{output_file.name}: expected 96 timestamps, "
                f"found {unique_timestamps}."
            )

    finally:
        connection.close()

    # Verify that all 96 expected timestamps are present.
    dataframe = pd.read_parquet(
        output_file,
        columns=["timestamp"]
    )

    timestamps = pd.to_datetime(
        dataframe["timestamp"]
    ).drop_duplicates()

    if timestamps.dt.tz is not None:
        timestamps = (
            timestamps.dt.tz_convert(IST)
            .dt.tz_localize(None)
        )

    actual_timestamps = {
        timestamp.to_pydatetime()
        for timestamp in timestamps
    }

    expected_timestamps = {
        datetime.combine(
            target_date,
            time(hour=hour, minute=minute)
        )
        for hour in range(24)
        for minute in (0, 15, 30, 45)
    }

    if actual_timestamps != expected_timestamps:
        raise ValueError(
            f"{output_file.name}: existing daily output "
            "does not contain the expected 96 timestamps."
        )

    logger.info(
        "Existing daily output validated: %s",
        target_date
    )


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

    logger.info("Found %d batches for %s.", len(batch_files), target_date)

    validate_batches(batch_files)
    validate_daily_timestamps(batch_files, target_date)

    logger.info("All individual batches passed validation for %s.", target_date)

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
    
    # Remove an orphaned temporary output from a previous
    # interrupted consolidation attempt.
    if temporary_file.exists():
        logger.warning("Removing incomplete temporary consolidation file: %s", temporary_file)
        temporary_file.unlink()

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

        logger.info(
            "Daily consolidation successful | date=%s | "
            "records=%d | unique_readings=%d | "
            "timestamps=%d | output=%s",
            target_date,
            total_records,
            unique_readings,
            unique_timestamps,
            output_file
        )

        return output_file

    except Exception:
        logger.exception(
            "Daily consolidation failed for date=%s",
            target_date
        )

        if temporary_file.exists():
            temporary_file.unlink()

        raise

    finally:
        connection.close()




def recover_pending_days():
    """
    Discover every date between the first available raw
    telemetry date and the latest completed checkpoint date.

    Consolidate missing daily outputs chronologically.
    Stop if a date folder is missing or invalid.
    """

    logger.info("Starting daily consolidation recovery.")

    last_completed_date = get_last_completed_date()

    if last_completed_date is None:
        logger.warning("No completed telemetry dates available.")

        return {
            "consolidated": [],
            "skipped": []
        }

    logger.info(
        "Latest completed date: %s",
        last_completed_date
    )

    if not RAW_DATA_DIR.exists():
        logger.warning("Raw telemetry directory does not exist.")

        return {
            "consolidated": [],
            "skipped": []
        }

    raw_dates = []

    for folder in RAW_DATA_DIR.glob("date=*"):

        if not folder.is_dir():
            continue

        date_string = folder.name.removeprefix("date=")

        try:
            folder_date = date.fromisoformat(date_string)

        except ValueError as error:
            raise ValueError(
                f"Invalid telemetry date directory: {folder.name}"
            ) from error

        raw_dates.append(folder_date)

    if not raw_dates:
        logger.warning("No raw telemetry date folders found.")

        return {
            "consolidated": [],
            "skipped": []
        }

    first_date = min(raw_dates)

    if first_date > last_completed_date:
        logger.info("No fully completed days available for consolidation.")

        return {
            "consolidated": [],
            "skipped": []
        }

    logger.info(
        "First available date: %s",
        first_date
    )

    consolidated = []
    skipped = []

    target_date = first_date

    while target_date <= last_completed_date:

        date_folder = (
            RAW_DATA_DIR / f"date={target_date}"
        )

        output_file = (
            DAILY_DATA_DIR /
            f"telemetry_{target_date}.parquet"
        )

        logger.info("Checking date: %s", target_date)

        # Detect an entirely missing raw date folder.
        if not date_folder.exists():

            raise FileNotFoundError(
                f"Missing historical telemetry directory: "
                f"{date_folder}. Recovery stopped."
            )

        # Validate existing daily outputs before skipping.
        if output_file.exists():

            validate_daily_output(
                output_file,
                target_date
            )

            skipped.append(target_date)

            logger.info("Existing daily output validated. Skipping.")

        else:

            logger.info("Daily output missing. Starting consolidation.")

            consolidate_day(target_date)

            validate_daily_output(
                output_file,
                target_date
            )

            consolidated.append(target_date)

        target_date += timedelta(days=1)

    logger.info(
        "Recovery summary | newly_consolidated=%d | skipped=%d",
        len(consolidated),
        len(skipped)
    )

    return {
        "consolidated": consolidated,
        "skipped": skipped
    }



if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Consolidate completed data center telemetry days."
    )

    parser.add_argument(
        "--date",
        type=parse_target_date,
        help="Consolidate one specific date in YYYY-MM-DD format."
    )

    args = parser.parse_args()

    lock_path = (
        CHECKPOINT_FILE.parent / "daily_consolidation.lock"
    )

    with exclusive_process_lock(lock_path) as lock_acquired:

        if not lock_acquired:
            raise SystemExit(0)

        if args.date:

            logger.info(
                "Target consolidation date: %s",
                args.date
            )

            consolidate_day(args.date)

        else:

            recover_pending_days()