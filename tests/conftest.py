import logging

import pytest

import utils.logger as logger_module
from utils.logger import get_logger


@pytest.fixture(autouse=True)
def isolate_application_logs(tmp_path, monkeypatch):
    """
    Redirect application logs to a temporary directory during tests.
    Prevents test runs from polluting operational log files.
    """
    monkeypatch.setattr(logger_module, "LOG_DIR", tmp_path)

    application_loggers = {
        "telemetry_pipeline": "telemetry_pipeline.log",
        "daily_consolidation": "daily_consolidation_structured.log",
        "device_state": "device_state.log",
        "process_lock": "process_lock.log",
        "silver_transform": "silver_transform.log",
        "gold_dimensions": "gold_dimensions.log",
        "gold_facts": "gold_facts.log",
    }

    configured_loggers = []

    for name, filename in application_loggers.items():
        logger = logging.getLogger(name)

        # Remove handlers pointing to previous log destinations.
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

        # Reconfigure the same logger object used by application modules.
        get_logger(name, filename)
        configured_loggers.append(logger)

    yield

    # Close temporary handlers after each test.
    for logger in configured_loggers:
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)