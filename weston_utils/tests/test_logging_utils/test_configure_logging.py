import logging
from pathlib import Path  # Used implicitly via tmp_path from pytest
import pytest
from weston_utils.logging_utils import configure_logging


# --------------------------------------------------------------------------------
### Tests basic log file creation + logging behavior
# --------------------------------------------------------------------------------
def test_logger_creates_log_file_and_logs(tmp_path):
    # Setup
    log_dir = tmp_path / "logs"
    log_file = "test_log"
    level = logging.DEBUG

    # Exercise
    logger = configure_logging(str(log_dir), log_file, level=level)
    logger.debug("This is a debug message.")

    # Verify: File was created and contains correct content
    log_path = log_dir / (log_file + ".log")
    assert log_path.exists(), "Log file was not created"

    contents = log_path.read_text()
    assert "This is a debug message." in contents
    assert "DEBUG" in contents


# --------------------------------------------------------------------------------
### Ensures `.log` extension is automatically appended
# --------------------------------------------------------------------------------
def test_logger_appends_log_extension(tmp_path):
    log_dir = tmp_path / "logs"
    log_file = "no_extension"  # Intentionally omit extension

    logger = configure_logging(str(log_dir), log_file)
    logger.info("Log file extension test")

    log_path = log_dir / "no_extension.log"
    assert log_path.exists()
    assert "Log file extension test" in log_path.read_text()


# --------------------------------------------------------------------------------
### Confirms function raises on missing required arguments
# --------------------------------------------------------------------------------
def test_logger_raises_on_missing_arguments():
    # Missing log_dir
    with pytest.raises(ValueError, match="log_dir"):
        configure_logging(None, "something")

    # Missing log_file
    with pytest.raises(ValueError, match="log_file"):
        configure_logging("logs", None)


# --------------------------------------------------------------------------------
### Tests dual-stream logging: stdout/stderr AND file output
# --------------------------------------------------------------------------------
def test_log_message_goes_to_console_and_file(tmp_path, capsys):
    log_dir = tmp_path / "logs"
    log_file = "console_file_test"

    logger = configure_logging(str(log_dir), log_file, level=logging.INFO)
    logger.info("Dual-stream test message")

    # Capture and check stderr (StreamHandler logs to stderr by default)
    captured = capsys.readouterr()
    assert "Dual-stream test message" in captured.err

    # Check file contents
    log_path = log_dir / (log_file + ".log")
    assert log_path.exists()
    assert "Dual-stream test message" in log_path.read_text()


# --------------------------------------------------------------------------------
### Ensures the logger returned is the root logger
# --------------------------------------------------------------------------------
def test_logger_returns_root_logger(tmp_path):
    log_dir = tmp_path / "logs"
    log_file = "root_test"

    logger = configure_logging(str(log_dir), log_file)
    root_logger = logging.getLogger()
    assert logger is root_logger