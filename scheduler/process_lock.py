from contextlib import contextmanager
from pathlib import Path
import fcntl


@contextmanager
def exclusive_process_lock(lock_path: Path):
    """
    Allow only one process to hold a lock file at a time.

    The lock is automatically released when the process exits,
    including when it encounters an exception.
    """

    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with lock_path.open("a+") as lock_file:
        try:
            fcntl.flock(
                lock_file.fileno(),
                fcntl.LOCK_EX | fcntl.LOCK_NB
            )
        except BlockingIOError:
            print(
                "Another pipeline instance is already running. "
                "Skipping this execution."
            )
            yield False
            return

        print("Pipeline execution lock acquired.")

        try:
            yield True
        finally:
            fcntl.flock(
                lock_file.fileno(),
                fcntl.LOCK_UN
            )
            print("Pipeline execution lock released.")