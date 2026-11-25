import json

from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd

from telemetry_generator.pipeline_runner import PipelineRunner
import telemetry_generator.pipeline_runner as pipeline_module
import telemetry_generator.device_state as device_state_module


IST = ZoneInfo("Asia/Kolkata")


def test_interrupted_historical_backfill():

    print("\nTEST: Interrupted historical backfill")

    with TemporaryDirectory() as temp_dir:

        root = Path(temp_dir)

        raw_dir = root / "raw"
        checkpoint_dir = root / "checkpoints"
        checkpoint_file = checkpoint_dir / "pipeline_checkpoint.json"
        state_dir = root / "device_states"

        patches = [
            patch.object(pipeline_module, "RAW_DATA_DIR", raw_dir),
            patch.object(pipeline_module, "CHECKPOINT_DIR", checkpoint_dir),
            patch.object(pipeline_module, "CHECKPOINT_FILE", checkpoint_file),
            patch.object(device_state_module, "STATE_DIR", state_dir),
        ]

        for item in patches:
            item.start()

        try:

            # Initialize the pipeline using temporary storage.
            runner = PipelineRunner()

            runner.state_manager.save_states()

            baseline = datetime(
                2026, 11, 26, 0, 0,
                tzinfo=IST
            )

            runner.save_checkpoint(baseline, 500, 0)

            original_run_once = runner.run_once
            execution_count = 0

            def simulate_interruption(timestamp):

                nonlocal execution_count

                execution_count += 1

                if execution_count == 1:

                    return original_run_once(timestamp)

                raise RuntimeError(
                    "Simulated interruption during backfill"
                )

            # Process the first interval, then simulate interruption.
            with patch.object(
                runner,
                "run_once",
                side_effect=simulate_interruption
            ):

                try:
                    runner.run_pending_intervals(
                        current_timestamp=datetime(
                            2026, 11, 26, 0, 45,
                            tzinfo=IST
                        )
                    )

                except RuntimeError as error:
                    print(error)

            # Only the first interval should have been committed.
            checkpoint = json.loads(
                checkpoint_file.read_text()
            )

            assert (
                checkpoint["last_successful_timestamp"]
                == "2026-11-26T00:15:00+05:30"
            )

            # Restart the pipeline.
            recovered_runner = PipelineRunner()

            # Process the remaining intervals.
            processed_count = recovered_runner.run_pending_intervals(
                current_timestamp=datetime(
                    2026, 11, 26, 0, 45,
                    tzinfo=IST
                )
            )

            assert processed_count == 2

            # Verify all three Parquet batches.
            expected_timestamps = [
                datetime(2026, 11, 26, 0, 15, tzinfo=IST),
                datetime(2026, 11, 26, 0, 30, tzinfo=IST),
                datetime(2026, 11, 26, 0, 45, tzinfo=IST),
            ]

            total_records = 0

            for timestamp in expected_timestamps:

                batch_path = recovered_runner.get_batch_path(
                    timestamp
                )

                assert batch_path.exists()

                batch = pd.read_parquet(batch_path)

                assert len(batch) == 500
                assert batch["server_id"].nunique() == 500

                total_records += len(batch)

            # Verify the final checkpoint.
            final_checkpoint = json.loads(
                checkpoint_file.read_text()
            )

            assert (
                final_checkpoint["last_successful_timestamp"]
                == "2026-11-26T00:45:00+05:30"
            )

            assert total_records == 1500

            print("\nAll three batches successfully verified.")
            print("Total records:", total_records)
            print("Final checkpoint:", final_checkpoint[
                "last_successful_timestamp"
            ])

            print("\nInterrupted backfill recovery passed!")

        finally:

            for item in reversed(patches):
                item.stop()


if __name__ == "__main__":

    test_interrupted_historical_backfill()

    print("\nALL BACKFILL EXECUTION TESTS PASSED!")