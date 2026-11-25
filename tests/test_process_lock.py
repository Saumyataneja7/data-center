from pathlib import Path
from tempfile import TemporaryDirectory

from scheduler.process_lock import exclusive_process_lock


def test_process_lock():

    with TemporaryDirectory() as temp_dir:

        lock_path = Path(temp_dir) / "pipeline.lock"

        # First process acquires the lock.
        with exclusive_process_lock(lock_path) as first_lock:

            assert first_lock is True
            print("First lock acquisition passed.")

            # Second process must not acquire the same lock.
            with exclusive_process_lock(lock_path) as second_lock:

                assert second_lock is False
                print("Concurrent lock rejection passed.")

        # Once the first process releases the lock,
        # another process should be able to acquire it.
        with exclusive_process_lock(lock_path) as third_lock:

            assert third_lock is True
            print("Lock release and reacquisition passed.")

    print("\nAll process lock tests passed!")


if __name__ == "__main__":
    test_process_lock()