# configure_logging.py

import logging
import os

# ===========================================
### UTILITY FUNCTION FOR LOGGING
# ===========================================
logger = logging.getLogger(__name__)

def configure_logging(log_dir, log_file, level=logging.INFO):
    """
    Configures logging to both a file and the console with a standardized format.

    Parameters:
        log_dir (str): Directory where the log file will be saved.
        log_file (str): Base name of the log file (without extension).
        level (int): Logging level (e.g., logging.INFO, logging.DEBUG).

    Returns:
        logging.Logger: Configured logger instance.

    Notes:
        - Ensures the log directory exists.
        - Appends ".log" to the provided log_file name.
        - Clears any existing root logger handlers (useful in notebooks or repeated script runs).
        - Logs messages to both a file and the console.
    """

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

    logger.info(f"These logs are saved in '{log_path}'")
    return logger