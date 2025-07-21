# utils/pg_database_utils/validate_postgres_general_identifier.py

from typing import Union, Iterable
import re
import logging

logger = logging.getLogger(__name__)

# ============================================================================================================
### Function that validates a list of strings as safe SQL identifiers (e.g. database name, table, owner, etc)
# ============================================================================================================
def validate_postgres_general_identifier(
    names: Union[str, Iterable[str]]
) -> list[str]:
    """
    Validate one or more strings as safe PostgreSQL column or variable names.

    This function checks identifiers against PostgreSQL naming conventions:
        - Must be strings
        - Begin with a letter (a–z or A–Z) or underscore (_)
        - Contain only letters, digits (0–9), or underscores

    Parameters:
        names (Union[str, Iterable[str]]): A single identifier or a list/iterable of identifiers.

    Returns:
        list[str]: The validated list of identifiers if all pass.

    Raises:
        ValueError: If any identifier is invalid, with reasons included in the message.

    Example:
        ">>> validate_postgres_column_identifier("user_id")
        ['user_id']

        ">>> validate_postgres_column_identifier(["valid_1", "123bad"])
        ValueError: Invalid SQL identifiers: '123bad' (RegexMismatch)
    """

    pattern = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    invalid = []

    if isinstance(names, str):
        names = [names]  # Coerce single string to list

    for name in names:
        if not isinstance(name, str):
            logger.error("Validation failed: Identifier is not a string: %r", name)
            invalid.append((name, "TypeError"))
            continue

        if not pattern.match(name):
            logger.error("Unsafe SQL identifier rejected: %r", name)
            invalid.append((name, "RegexMismatch"))
        else:
            logger.info("Identifier validated as safe: %s", name)

    if invalid:
        failed_names = ", ".join(f"{name!r} ({reason})" for name, reason in invalid)
        raise ValueError(f"Invalid SQL identifiers: {failed_names}")

    return list(names)

__all__ = [
    "validate_postgres_general_identifier"
]