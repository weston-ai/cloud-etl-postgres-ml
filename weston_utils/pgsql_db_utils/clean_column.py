# utils/pg_database_utils/clean_column.py

# No modules required for this function (using Python 3.11.11)

# ==============================================================================
### Function that standardizes the column headers to a clean lowercase format
# ==============================================================================
def clean_column(col: str, max_length: int = 63) -> str:
    """
    Clean a column name to conform with PostgreSQL naming conventions.

    This function standardizes a string to be used as a PostgreSQL column name by:
      - Stripping leading and trailing whitespace
      - Converting to lowercase
      - Replacing spaces, hyphens (`-`), and colons (`:`) with underscores (`_`)
      - Truncating to a maximum length (default: 63 characters, per PostgreSQL limit)

    Parameters:
        col (str): The original column name.
        max_length (int, optional): Maximum length for the cleaned name.
                                    Defaults to 63 (PostgreSQL's column name limit).

    Returns:
        str: A cleaned and truncated column name suitable for PostgreSQL.

    Dependencies:
        This function uses only the Python standard library.
    """

    cleaned = (
        col.strip()
           .lower()
           .replace(" ", "_")
           .replace("-", "_")
           .replace(":", "_")
    )
    return cleaned[:max_length]

__all__ = [
    "clean_column"
]