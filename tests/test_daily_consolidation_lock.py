from pathlib import Path
from tempfile import TemporaryDirectory

from scheduler.process_lock import exclusive_process_lock


def test_daily_consolidation_lock_is_independent():

    with TemporaryDirectory() as temp_dir:

        checkpoint_dir = Path(temp_dir) / "checkpoints"

        pipeline_lock = checkpoint_dir / "pipeline.lock"
        consolidation_lock = (
            checkpoint_dir / "daily_consolidation.lock"
        )

        # Acquire the telemetry pipeline lock.
        with exclusive_process_lock(pipeline_lock) as pipeline_acquired:

            assert pipeline_acquired is True

            # Daily consolidation must still be able to run.
            with exclusive_process_lock(
                consolidation_lock
            ) as consolidation_acquired:

                assert consolidation_acquired is True

                # A second consolidation instance must be rejected.
                with exclusive_process_lock(
                    consolidation_lock
                ) as second_consolidation:

                    assert second_consolidation is False

            # Pipeline lock remains held independently.
            assert pipeline_lock.exists()

        # Both locks should now be available.
        with exclusive_process_lock(
            consolidation_lock
        ) as reacquired:

            assert reacquired is True


if __name__ == "__main__":
    test_daily_consolidation_lock_is_independent()