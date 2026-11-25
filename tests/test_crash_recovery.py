from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from zoneinfo import ZoneInfo

import json

from telemetry_generator.pipeline_runner import PipelineRunner
import telemetry_generator.device_state as device_state_module
import telemetry_generator.pipeline_runner as pipeline_module


IST = ZoneInfo("Asia/Kolkata")

TEST_TIMESTAMP = datetime(
    2026, 11, 26, 0, 15, tzinfo=IST
)


def create_test_runner(temp_dir):

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

    runner = PipelineRunner()

    # Persist the initial baseline state.
    runner.state_manager.save_states()

    # Establish a previous committed checkpoint.
    runner.save_checkpoint(
        datetime(2026, 11, 26, 0, 0, tzinfo=IST),
        500,
        18
    )

    return runner, patches, checkpoint_file, checkpoint_dir


def stop_patches(patches):

    for item in reversed(patches):
        item.stop()


def test_crash_after_parquet_before_pending():

    print("\nTEST 1: Crash after Parquet write")

    with TemporaryDirectory() as temp_dir:

        runner, patches, checkpoint_file, checkpoint_dir = (
            create_test_runner(temp_dir)
        )

        try:

            # Simulate a crash before the pending transaction is saved.
            with patch.object(
                runner,
                "save_pending_transaction",
                side_effect=RuntimeError("Simulated crash")
            ):

                try:
                    runner.run_once(TEST_TIMESTAMP)
                except RuntimeError:
                    print("Simulated crash successfully triggered.")

            batch_path = runner.get_batch_path(TEST_TIMESTAMP)

            assert batch_path.exists()
            assert not runner.pending_transaction_file.exists()

            # Restart the pipeline.
            recovered_runner = PipelineRunner()

            # The orphan batch should be deterministically regenerated
            # and safely replaced.
            recovered_runner.run_once(TEST_TIMESTAMP)

            assert checkpoint_file.exists()
            assert not recovered_runner.pending_transaction_file.exists()

            checkpoint = json.loads(
                checkpoint_file.read_text()
            )

            assert (
                checkpoint["last_successful_timestamp"]
                == TEST_TIMESTAMP.isoformat()
            )

            assert len(
                __import__("pandas").read_parquet(batch_path)
            ) == 500

            print("Orphan batch replay passed.")

        finally:
            stop_patches(patches)

    print("TEST 1 PASSED!")


def test_crash_after_pending_before_checkpoint():

    print("\nTEST 2: Crash after pending transaction")

    with TemporaryDirectory() as temp_dir:

        runner, patches, checkpoint_file, checkpoint_dir = (
            create_test_runner(temp_dir)
        )

        try:

            # Simulate a crash after state persistence but before
            # the checkpoint can be updated.
            with patch.object(
                runner,
                "save_checkpoint",
                side_effect=RuntimeError("Simulated crash")
            ):

                try:
                    runner.run_once(TEST_TIMESTAMP)
                except RuntimeError:
                    print("Simulated crash successfully triggered.")

            assert runner.pending_transaction_file.exists()

            # The checkpoint should still refer to 00:00.
            old_checkpoint = json.loads(
                checkpoint_file.read_text()
            )

            assert (
                old_checkpoint["last_successful_timestamp"]
                == "2026-11-26T00:00:00+05:30"
            )

            # Restart. PipelineRunner should recover automatically
            # during initialization.
            recovered_runner = PipelineRunner()

            assert not recovered_runner.pending_transaction_file.exists()

            new_checkpoint = json.loads(
                checkpoint_file.read_text()
            )

            assert (
                new_checkpoint["last_successful_timestamp"]
                == TEST_TIMESTAMP.isoformat()
            )

            print("Pending transaction recovery passed.")

        finally:
            stop_patches(patches)

    print("TEST 2 PASSED!")


if __name__ == "__main__":

    test_crash_after_parquet_before_pending()

    test_crash_after_pending_before_checkpoint()

    print("\nALL CRASH RECOVERY TESTS PASSED!")