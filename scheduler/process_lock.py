from contextlib import contextmanager
from pathlib import Path
import fcntl

from utils.logger import get_logger


logger = get_logger(
    "process_lock",
    "process_lock.log"
)

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
            logger.warning(
                "Lock already held by another process. "
                "Skipping execution | lock=%s",
                lock_path
            )
            yield False
            return

        logger.info(
            "Process execution lock acquired | lock=%s",
            lock_path
        )

        try:
            yield True
        finally:
            fcntl.flock(
                lock_file.fileno(),
                fcntl.LOCK_UN
            )
            logger.info(
                "Process execution lock released | lock=%s",
                lock_path
            )