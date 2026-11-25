from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from etl.daily_consolidation import (
    get_expected_batch_names,
    consolidate_day,
)


def test_expected_batch_count():
    batches = get_expected_batch_names()

    assert len(batches) == 96
    assert "batch=0000.parquet" in batches
    assert "batch=2345.parquet" in batches

    print("Expected batch count test passed.")


def test_incomplete_day_rejected():

    target_date = date(2026, 11, 25)

    with TemporaryDirectory() as temp_dir:

        temporary_raw_dir = Path(temp_dir)

        date_folder = (
            temporary_raw_dir / f"date={target_date}"
        )

        date_folder.mkdir(parents=True)

        # Create only one placeholder batch.
        (date_folder / "batch=1830.parquet").touch()

        with patch(
            "etl.daily_consolidation.RAW_DATA_DIR",
            temporary_raw_dir
        ):

            try:
                consolidate_day(target_date)

            except ValueError as error:
                assert "Missing 95 batches" in str(error)
                print("Incomplete-day rejection test passed.")

            else:
                raise AssertionError(
                    "Incomplete day should have been rejected."
                )


if __name__ == "__main__":
    test_expected_batch_count()
    test_incomplete_day_rejected()

    print("\nAll daily consolidation tests passed!")