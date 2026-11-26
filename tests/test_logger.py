import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from uuid import uuid4

import utils.logger as logger_module
from utils.logger import get_logger


def create_test_logger(tmp_path):
    """Create an isolated logger for testing."""

    filename = f"test_{uuid4().hex}.log"
    logger_name = f"test_logger_{uuid4().hex}"

    logger = get_logger(
        logger_name,
        filename
    )

    log_path = tmp_path / filename

    return logger, log_path


def cleanup_logger(logger):
    """Close and remove handlers after each test."""

    for handler in logger.handlers[:]:
        handler.flush()
        handler.close()
        logger.removeHandler(handler)


def test_logger_writes_structured_messages(
    tmp_path,
    monkeypatch
):

    monkeypatch.setattr(
        logger_module,
        "LOG_DIR",
        tmp_path
    )

    logger, log_path = create_test_logger(tmp_path)

    try:
        logger.info("Test pipeline started")
        logger.warning("Test warning")
        logger.error("Test error")

        for handler in logger.handlers:
            handler.flush()

        assert log_path.exists()

        content = log_path.read_text(
            encoding="utf-8"
        )

        assert "INFO" in content
        assert "WARNING" in content
        assert "ERROR" in content
        assert "Test pipeline started" in content
        assert logger.name in content
        assert "+05:30" in content

    finally:
        cleanup_logger(logger)


def test_logger_captures_exception_traceback(
    tmp_path,
    monkeypatch
):

    monkeypatch.setattr(
        logger_module,
        "LOG_DIR",
        tmp_path
    )

    logger, log_path = create_test_logger(tmp_path)

    try:
        try:
            raise ValueError("Simulated pipeline failure")

        except ValueError:
            logger.exception(
                "Pipeline execution failed."
            )

        for handler in logger.handlers:
            handler.flush()

        content = log_path.read_text(
            encoding="utf-8"
        )

        assert "ERROR" in content
        assert "Pipeline execution failed." in content
        assert "Traceback (most recent call last)" in content
        assert "ValueError: Simulated pipeline failure" in content

    finally:
        cleanup_logger(logger)


def test_logger_rotation_and_backup_limit(
    tmp_path,
    monkeypatch
):

    monkeypatch.setattr(
        logger_module,
        "LOG_DIR",
        tmp_path
    )

    logger, log_path = create_test_logger(tmp_path)

    try:
        file_handler = next(
            handler
            for handler in logger.handlers
            if isinstance(handler, RotatingFileHandler)
        )

        # Verify production configuration.
        assert file_handler.maxBytes == 5 * 1024 * 1024
        assert file_handler.backupCount == 5

        # Use smaller values to test rotation quickly.
        file_handler.maxBytes = 300
        file_handler.backupCount = 2

        for index in range(100):
            logger.info(
                "Rotation test message %03d: %s",
                index,
                "X" * 80
            )

        file_handler.flush()

        assert log_path.exists()
        assert Path(str(log_path) + ".1").exists()

        backup_files = list(
            tmp_path.glob(log_path.name + ".*")
        )

        assert len(backup_files) <= 2

    finally:
        cleanup_logger(logger)


def test_logger_does_not_duplicate_handlers(
    tmp_path,
    monkeypatch
):

    monkeypatch.setattr(
        logger_module,
        "LOG_DIR",
        tmp_path
    )

    filename = f"test_{uuid4().hex}.log"
    logger_name = f"test_logger_{uuid4().hex}"

    first_logger = get_logger(
        logger_name,
        filename
    )

    try:
        initial_handler_count = len(
            first_logger.handlers
        )

        second_logger = get_logger(
            logger_name,
            filename
        )

        assert first_logger is second_logger

        assert len(second_logger.handlers) == (
            initial_handler_count
        )

        second_logger.info(
            "This message should appear only once."
        )

        for handler in second_logger.handlers:
            handler.flush()

        log_path = tmp_path / filename

        content = log_path.read_text(
            encoding="utf-8"
        )

        assert content.count(
            "This message should appear only once."
        ) == 1

    finally:
        cleanup_logger(first_logger)