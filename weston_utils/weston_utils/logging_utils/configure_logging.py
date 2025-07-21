# configure_logging.py

import logging
import os

# ===========================================
### UTILITY FUNCTION FOR LOGGING
# ===========================================
def configure_logging(
        log_dir=None,
        log_file=None,
        level=logging.INFO
) -> None:
    """
    Configure a logger that writes to both a file and the console with standardized formatting.

    This function sets up dual logging streams: one to a log file and one to the console (stdout).
    It ensures the log directory exists, appends a `.log` extension to the filename, clears any
    pre-existing root handlers (important in Jupyter or reloaded modules), and applies a consistent
    format to both outputs.

    Parameters:
        log_dir (str):
            Directory where the log file will be saved. Created if it does not exist.
        log_file (str):
            Base name of the log file (without the `.log` extension).
        level (int, optional):
            Logging level (e.g., `logging.INFO`, `logging.DEBUG`). Defaults to `logging.INFO`.

    Returns:
        logging.Logger:
            A logger instance configured to write to both file and console.

    Behavior:
        - Ensures the specified log directory exists.
        - Constructs the log path as: log_dir / (log_file + ".log")
        - Clears any existing root logger handlers to avoid duplicate logs.
        - Applies the format: "%(asctime)s - %(levelname)s - %(message)s"
        - Logs a startup message to confirm the log path.

    Example:
        " >>> logger = configure_logging("logs/setup_logs", "create_db", level=logging.DEBUG) "
        " >>> logger.info("Logging system initialized.") "

    Dependencies:
        - Python standard libraries: os, logging

    Python Compatibility:
        - Compatible with Python 3.6+
        - Developed and tested on Python 3.11.11
    """

    # Make sure real variables are passed to function
    required_args = {
        "log_dir": log_dir,
        "log_file": log_file
    }

    for name, value in required_args.items():
        if value is None:
            raise ValueError(f"Missing required argument: '{name}' cannot be None.")

    # Configure logging directory
    os.makedirs(log_dir, exist_ok=True) # ensure filepath exists
    log_path = os.path.join(log_dir, (log_file + ".log"))

    # Clear root logger handlers (critical in notebooks or repeated scripts)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Define format and handlers
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(level) # level dynamically set

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)  # level dynamically set

    logging.basicConfig(
        level=level,  # level dynamically set
        format=log_format,
        handlers=[file_handler, stream_handler]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"These logs are saved in '{log_path}'")
    return logger