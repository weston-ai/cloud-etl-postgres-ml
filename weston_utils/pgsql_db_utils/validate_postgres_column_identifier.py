# utils/pg_database_utils/validate_postgres_column_identifier.py

import logging
from typing import Iterable
import re

logger = logging.getLogger(__name__)

# ============================================================================================
### Function that validates a list of strings as safe COLUMN IDENTIFIERS for a Postgres table
# ============================================================================================
def validate_postgres_column_identifier(names) -> list[str]:
    """
    Validate a list of strings as safe PostgreSQL column or variable names.

    This function checks each identifier against PostgreSQL naming conventions to ensure
    safety for use in raw SQL execution. Valid identifiers must:
        - Be strings
        - Begin with a letter (a–z or A–Z) or underscore (_)
        - Contain only letters, digits (0–9), or underscores
        - Be no longer than 63 characters

    Invalid identifiers are logged and collected, and a ValueError is raised summarizing
    all failures after validation completes.

    Parameters:
        names (Iterable[str]):
            A list or iterable of candidate SQL identifier strings.

    Returns:
        List[str]:
            The original list of validated identifiers, if all are valid.

    Raises:
        ValueError:
            If any identifier fails validation. The exception lists each invalid name
            along with its reason (e.g., "TypeError", "RegexMismatch").

    Logging:
        - Logs each identifier as either valid or invalid
        - Logs reasons for failure, including type mismatch or pattern mismatch

    Example:
        " >>> validate_postgres_column_identifier(["column_1", "userID", "123bad"])
        ValueError: Invalid SQL identifiers: '123bad' (RegexMismatch)

    Dependencies:
        - Python standard libraries: re, logging

    Python Compatibility:
        - Compatible with Python 3.6+
        - Developed and tested on Python 3.11.11
    """

    pattern = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")  # First character must be a letter, and only letters, numbers or underscores are allowed. Length limit of 63, which adheres to Postgres length limit.
    invalid = []

    for name in names:
        if not isinstance(name, str):
            logger.error(f"Validation failed: Identifier is not a string: {name!r}")
            invalid.append((name, "TypeError"))
            continue

        if not pattern.match(name):
            logger.error(f"Unsafe SQL identifier rejected: {name}")
            invalid.append((name, "RegexMismatch"))
        else:
            logger.info(f"Identifier validated as safe: {name}")

    if invalid:
        failed_names = ", ".join(f"{name!r} ({reason})" for name, reason in invalid)
        raise ValueError(f"Invalid SQL identifiers: {failed_names}")

    return list(names)

__all__ = [
    "validate_postgres_column_identifier"
]