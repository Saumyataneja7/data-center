import json

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch, call

import etl.daily_consolidation as consolidation


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


def create_date_folder(raw_dir, target_date):

    folder = raw_dir / f"date={target_date}"
    folder.mkdir(parents=True, exist_ok=True)

    return folder


def test_multiple_day_recovery():

    print("\nTEST 1: Multiple-day consolidation recovery")

    with TemporaryDirectory() as temp_dir:

        root = Path(temp_dir)

        raw_dir = root / "raw"
        daily_dir = root / "daily"

        raw_dir.mkdir()
        daily_dir.mkdir()

        checkpoint_file = (
            root / "checkpoints" / "pipeline_checkpoint.json"
        )

        create_checkpoint(
            checkpoint_file,
            "2026-11-29T00:00:00+05:30"
        )

        dates = [
            date(2026, 11, 25),
            date(2026, 11, 26),
            date(2026, 11, 27),
            date(2026, 11, 28),
            date(2026, 11, 29),
        ]

        for target_date in dates:
            create_date_folder(raw_dir, target_date)

        # November 25 was already consolidated.
        existing_output = (
            daily_dir / "telemetry_2026-11-25.parquet"
        )

        existing_output.touch()

        with (
            patch.object(consolidation, "RAW_DATA_DIR", raw_dir),
            patch.object(consolidation, "DAILY_DATA_DIR", daily_dir),
            patch.object(
                consolidation,
                "CHECKPOINT_FILE",
                checkpoint_file
            ),
            patch.object(
                consolidation,
                "validate_daily_output"
            ) as mock_validate,
            patch.object(
                consolidation,
                "consolidate_day"
            ) as mock_consolidate
        ):

            def fake_consolidate(target_date):

                output = (
                    daily_dir /
                    f"telemetry_{target_date}.parquet"
                )

                output.touch()

                return output

            mock_consolidate.side_effect = fake_consolidate

            result = consolidation.recover_pending_days()

            expected_consolidated = [
                date(2026, 11, 26),
                date(2026, 11, 27),
                date(2026, 11, 28),
            ]

            assert result["consolidated"] == expected_consolidated

            assert result["skipped"] == [
                date(2026, 11, 25)
            ]

            assert mock_consolidate.call_args_list == [
                call(target_date)
                for target_date in expected_consolidated
            ]

            assert date(2026, 11, 29) not in (
                result["consolidated"]
            )

            print("Previously consolidated day skipped.")
            print("Three historical days recovered.")
            print("Current incomplete day excluded.")

    print("TEST 1 PASSED!")


def test_recovery_stops_on_incomplete_day():

    print("\nTEST 2: Stop recovery on incomplete day")

    with TemporaryDirectory() as temp_dir:

        root = Path(temp_dir)

        raw_dir = root / "raw"
        daily_dir = root / "daily"

        raw_dir.mkdir()
        daily_dir.mkdir()

        checkpoint_file = (
            root / "checkpoints" / "pipeline_checkpoint.json"
        )

        create_checkpoint(
            checkpoint_file,
            "2026-11-28T00:00:00+05:30"
        )

        dates = [
            date(2026, 11, 25),
            date(2026, 11, 26),
            date(2026, 11, 27),
        ]

        for target_date in dates:
            create_date_folder(raw_dir, target_date)

        # First date already has a completed output.
        (
            daily_dir / "telemetry_2026-11-25.parquet"
        ).touch()

        with (
            patch.object(consolidation, "RAW_DATA_DIR", raw_dir),
            patch.object(consolidation, "DAILY_DATA_DIR", daily_dir),
            patch.object(
                consolidation,
                "CHECKPOINT_FILE",
                checkpoint_file
            ),
            patch.object(
                consolidation,
                "validate_daily_output"
            ),
            patch.object(
                consolidation,
                "consolidate_day"
            ) as mock_consolidate
        ):

            mock_consolidate.side_effect = ValueError(
                "Missing telemetry batches."
            )

            try:

                consolidation.recover_pending_days()

            except ValueError as error:

                assert "Missing telemetry batches" in str(error)

            else:

                raise AssertionError(
                    "Recovery should stop on an incomplete day."
                )

            assert mock_consolidate.call_args_list == [
                call(date(2026, 11, 26))
            ]

            print("Recovery stopped safely.")
            print("Later dates were not processed.")

    print("TEST 2 PASSED!")


def test_missing_historical_date_folder():

    print("\nTEST 3: Missing historical date folder")

    with TemporaryDirectory() as temp_dir:

        root = Path(temp_dir)

        raw_dir = root / "raw"
        daily_dir = root / "daily"

        raw_dir.mkdir()
        daily_dir.mkdir()

        checkpoint_file = (
            root / "checkpoints" / "pipeline_checkpoint.json"
        )

        create_checkpoint(
            checkpoint_file,
            "2026-11-28T00:00:00+05:30"
        )

        # November 25 and 27 exist.
        # November 26 is deliberately missing.
        (raw_dir / "date=2026-11-25").mkdir()
        (raw_dir / "date=2026-11-27").mkdir()

        # November 25 is already consolidated.
        # This ensures recovery skips it and reaches November 26.
        (
            daily_dir / "telemetry_2026-11-25.parquet"
        ).touch()

        with (
            patch.object(consolidation, "RAW_DATA_DIR", raw_dir),
            patch.object(consolidation, "DAILY_DATA_DIR", daily_dir),
            patch.object(
                consolidation,
                "CHECKPOINT_FILE",
                checkpoint_file
            ),
            patch.object(
                consolidation,
                "validate_daily_output"
            ) as mock_validate,
            patch.object(
                consolidation,
                "consolidate_day"
            ) as mock_consolidate
        ):

            mock_validate.return_value = None

            try:

                consolidation.recover_pending_days()

            except FileNotFoundError as error:

                assert "2026-11-26" in str(error)

                print("Missing historical date detected.")
                print("Recovery stopped safely.")

            else:

                raise AssertionError(
                    "Recovery should detect the missing date folder."
                )

            mock_consolidate.assert_not_called()

    print("TEST 3 PASSED!")


if __name__ == "__main__":

    test_multiple_day_recovery()

    test_recovery_stops_on_incomplete_day()

    test_missing_historical_date_folder()

    print("\nALL CONSOLIDATION RECOVERY TESTS PASSED!")