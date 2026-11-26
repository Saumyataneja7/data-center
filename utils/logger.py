import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"

IST = ZoneInfo("Asia/Kolkata")


class ISTFormatter(logging.Formatter):
    """Format log timestamps using Indian Standard Time."""

    def formatTime(self, record, datefmt=None):
        timestamp = datetime.fromtimestamp(
            record.created,
            tz=IST
        )

        if datefmt:
            return timestamp.strftime(datefmt)

        return timestamp.isoformat(timespec="milliseconds")


def get_logger(
    name: str,
    log_filename: str,
    level=logging.INFO
):
    """
    Create or retrieve a configured application logger.

    Logs are written to a rotating file and the console.
    """

    if Path(log_filename).name != log_filename:
        raise ValueError(
            "log_filename must be a filename, not a path."
        )

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    logger = logging.getLogger(name)

    logger.setLevel(level)
    logger.propagate = False

    # Prevent duplicate handlers if this function is called again.
    if logger.handlers:
        return logger

    formatter = ISTFormatter(
        fmt=(
            "%(asctime)s | %(levelname)-8s | "
            "%(name)s | %(message)s"
        )
    )

    log_path = LOG_DIR / log_filename

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )

    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)

    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger