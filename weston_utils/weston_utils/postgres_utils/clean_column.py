# utils/postgres_utils/clean_column.py

import re

# Optional: include a basic set of PostgreSQL reserved keywords
POSTGRES_RESERVED_KEYWORDS = {
    "user", "select", "from", "where", "group", "order", "limit", "table",
    "column", "insert", "delete", "update", "into", "create", "drop", "values",
    "join", "inner", "left", "right", "on", "and", "or", "not", "as", "by"
}

PYTHON_RESERVED_NAMES = {
    "class", "def", "lambda", "return", "global", "from", "import", "if",
    "for", "while", "with", "try", "except", "raise", "yield", "assert"
}

def clean_column(col: str, max_length: int = 63) -> str:
    """
    Normalize a string for use as a PostgreSQL column name.

    Rules:
    - Lowercase
    - Replace non-word characters with underscore
    - Collapse consecutive underscores
    - Remove leading/trailing whitespace
    - Must start with a letter or underscore
    - Cannot be empty (use 'col' fallback)
    - Truncate to max_length
    - Avoid reserved keywords by appending '_col'
    """

    # Defensive guard
    if not isinstance(col, str):
        raise TypeError("Column name must be a string")

    # Step 1: Strip whitespace and lowercase
    cleaned = col.strip()

    # Step 2: Insert underscores at camelCase and PascalCase boundaries
    #   - e.g. aA → a_A
    #   - e.g. HTTPServer → HTTP_Server
    cleaned = re.sub(r'(?<=[a-z])(?=[A-Z])', '_', cleaned)
    cleaned = re.sub(r'(?<=[A-Z])(?=[A-Z][a-z])', '_', cleaned)

    # Step 3: Force to lower case
    cleaned = cleaned.lower()

    # Step 3: Replace non-word characters with underscores
    cleaned = re.sub(r"[^\w]+", "_", cleaned)

    # Step 4: Collapse multiple underscores
    cleaned = re.sub(r"__+", "_", cleaned)

    # Step 5: Fallback if empty
    if not cleaned:
        cleaned = "col"

    # Step 6: Ensure valid start character (letter or underscore)
    if not re.match(r"^[a-z_]", cleaned):
        cleaned = f"col_{cleaned}"

    # Step 7: Truncate to max length
    cleaned = cleaned[:max_length]

    # Step 8: Avoid ending with underscore
    cleaned = cleaned.rstrip("_")

    # Step 9: Avoid Python and Postgres reserved words
    if cleaned in POSTGRES_RESERVED_KEYWORDS or cleaned in PYTHON_RESERVED_NAMES:
        cleaned += "_col"

    # Final fallback if everything has been stripped
    if not cleaned:
        cleaned = "col"

    return cleaned

__all__ = [
    "clean_column"
]