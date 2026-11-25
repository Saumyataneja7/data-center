import hashlib

from datetime import date, datetime, time
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd

import etl.daily_consolidation as consolidation


IST = ZoneInfo("Asia/Kolkata")

TARGET_DATE = date(2026, 11, 26)


def get_file_hash(file_path):

    return hashlib.sha256(
        file_path.read_bytes()
    ).hexdigest()


def create_complete_raw_day(raw_dir, target_date):

    date_folder = raw_dir / f"date={target_date}"
    date_folder.mkdir(parents=True)

    for hour in range(24):

        for minute in (0, 15, 30, 45):

            timestamp = datetime(
                target_date.year,
                target_date.month,
                target_date.day,
                hour,
                minute,
                tzinfo=IST
            )

            dataframe = pd.DataFrame({
                "server_id": [
                    f"SERVER_{server_number:03d}"
                    for server_number in range(500)
                ],
                "timestamp": [timestamp] * 500,
                "cpu_utilization": [45.0] * 500
            })

            batch_name = (
                f"batch={hour:02d}{minute:02d}.parquet"
            )

            dataframe.to_parquet(
                date_folder / batch_name,
                index=False
            )

    return date_folder


def test_interrupted_consolidation_recovery():

    print("\nTEST: Interrupted daily consolidation recovery")

    with TemporaryDirectory() as temp_dir:

        root = Path(temp_dir)

        raw_dir = root / "raw"
        daily_dir = root / "daily"

        raw_dir.mkdir()
        daily_dir.mkdir()

        # Generate a complete day of raw telemetry.
        date_folder = create_complete_raw_day(
            raw_dir,
            TARGET_DATE
        )

        batch_files = sorted(
            date_folder.glob("batch=*.parquet")
        )

        assert len(batch_files) == 96

        print("Created 96 raw telemetry batches.")

        # Capture a raw batch hash to verify that
        # consolidation does not modify raw data.
        first_batch = batch_files[0]
        original_hash = get_file_hash(first_batch)

        output_file = (
            daily_dir /
            f"telemetry_{TARGET_DATE}.parquet"
        )

        temporary_file = output_file.with_name(
            output_file.stem + ".tmp.parquet"
        )

        # Simulate a previous process crashing while
        # writing its temporary Parquet output.
        temporary_file.write_bytes(
            b"INTERRUPTED PARTIAL PARQUET FILE"
        )

        assert temporary_file.exists()
        assert not output_file.exists()

        print("Simulated interrupted temporary file.")

        with (
            patch.object(
                consolidation,
                "RAW_DATA_DIR",
                raw_dir
            ),
            patch.object(
                consolidation,
                "DAILY_DATA_DIR",
                daily_dir
            )
        ):

            # Restart daily consolidation.
            result = consolidation.consolidate_day(
                TARGET_DATE
            )

            assert result == output_file

            # Verify the final daily output.
            assert output_file.exists()

            dataframe = pd.read_parquet(output_file)

            assert len(dataframe) == 48000

            assert dataframe["server_id"].nunique() == 500

            assert dataframe["timestamp"].nunique() == 96

            # Verify the interrupted temporary file is gone.
            assert not temporary_file.exists()

            # Verify raw telemetry was not modified.
            assert get_file_hash(first_batch) == original_hash

            # Independently validate the final output.
            consolidation.validate_daily_output(
                output_file,
                TARGET_DATE
            )

            print("\nFinal daily output verified.")
            print("Total records:", len(dataframe))
            print("Unique servers:", dataframe["server_id"].nunique())
            print("Unique timestamps:", dataframe["timestamp"].nunique())
            print("Temporary file removed.")
            print("Original raw batch unchanged.")

    print("\nInterrupted consolidation recovery passed!")


if __name__ == "__main__":

    test_interrupted_consolidation_recovery()

    print("\nALL CONSOLIDATION CRASH RECOVERY TESTS PASSED!")