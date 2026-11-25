import hashlib
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from telemetry_generator.pipeline_runner import PipelineRunner
from telemetry_generator.config import CHECKPOINT_FILE, STATE_DIR


IST = ZoneInfo("Asia/Kolkata")


def get_file_hash(file_path):
    return hashlib.sha256(
        file_path.read_bytes()
    ).hexdigest()


def test_duplicate_batch_protection():

    existing_timestamp = datetime(
        2026, 11, 25, 19, 15,
        tzinfo=IST
    )

    runner = PipelineRunner()

    batch_path = runner.get_batch_path(existing_timestamp)
    state_file = STATE_DIR / "server_states.json"

    # Verify that the original batch exists.
    assert batch_path.exists(), (
        "Expected Parquet batch does not exist."
    )

    # Capture the original files before attempting duplicate processing.
    batch_hash_before = get_file_hash(batch_path)
    state_hash_before = get_file_hash(state_file)

    with open(CHECKPOINT_FILE, "r") as file:
        checkpoint_before = json.load(file)

    print("\nAttempting to process an already committed interval.")

    try:
        runner.run_once(existing_timestamp)

    except ValueError as error:

        assert "already committed" in str(error)

        print("Duplicate interval correctly rejected.")
        print(error)

    else:
        raise AssertionError(
            "Pipeline should reject an already committed interval."
        )

    # Verify that the original Parquet file was not modified.
    batch_hash_after = get_file_hash(batch_path)

    assert batch_hash_before == batch_hash_after

    print("Original Parquet batch remained unchanged.")

    # Verify that device states were not modified.
    state_hash_after = get_file_hash(state_file)

    assert state_hash_before == state_hash_after

    print("Device states remained unchanged.")

    # Verify that the checkpoint was not modified.
    with open(CHECKPOINT_FILE, "r") as file:
        checkpoint_after = json.load(file)

    assert checkpoint_before == checkpoint_after

    print("Checkpoint remained unchanged.")

    print("\nDuplicate batch protection test passed!")


if __name__ == "__main__":
    test_duplicate_batch_protection()