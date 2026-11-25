from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo
from unittest.mock import patch

import pandas as pd

from etl.daily_consolidation import consolidate_day
from telemetry_generator.device_simulator import create_device_registry


IST = ZoneInfo("Asia/Kolkata")


def test_complete_daily_consolidation():

    target_date = date(2026, 11, 26)

    servers = create_device_registry()

    with TemporaryDirectory() as temp_dir:

        temp_root = Path(temp_dir)

        raw_dir = temp_root / "raw"
        daily_dir = temp_root / "daily"

        date_folder = raw_dir / f"date={target_date}"
        date_folder.mkdir(parents=True)

        # Generate 96 test batches.
        for interval_number in range(96):

            timestamp = (
                datetime(
                    2026, 11, 26, 0, 0,
                    tzinfo=IST
                )
                + timedelta(minutes=15 * interval_number)
            )

            records = []

            for server in servers:
                records.append({
                    "server_id": server["server_id"],
                    "datacenter_id": server["datacenter_id"],
                    "timestamp": timestamp,
                    "cpu_utilization": 35.0,
                    "memory_utilization": 50.0,
                    "is_anomaly": False,
                })

            dataframe = pd.DataFrame(records)

            batch_name = (
                f"batch={timestamp:%H%M}.parquet"
            )

            dataframe.to_parquet(
                date_folder / batch_name,
                engine="pyarrow",
                index=False
            )

        print("Created 96 test batches.")

        # Redirect the consolidation script to temporary folders.
        with (
            patch(
                "etl.daily_consolidation.RAW_DATA_DIR",
                raw_dir
            ),
            patch(
                "etl.daily_consolidation.DAILY_DATA_DIR",
                daily_dir
            )
        ):

            output_file = consolidate_day(target_date)

        assert output_file.exists()

        result = pd.read_parquet(output_file)

        assert len(result) == 48000
        assert result["server_id"].nunique() == 500
        assert result["timestamp"].nunique() == 96

        print("\n========== COMPLETE DAY VALIDATION ==========")
        print(f"Total records: {len(result)}")
        print(f"Unique servers: {result['server_id'].nunique()}")
        print(f"Unique timestamps: {result['timestamp'].nunique()}")
        print(f"Output file: {output_file}")

        print("\nComplete daily consolidation test passed!")


if __name__ == "__main__":
    test_complete_daily_consolidation()