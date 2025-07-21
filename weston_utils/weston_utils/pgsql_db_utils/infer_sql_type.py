# utils/pg_database_utils/infer_sql_type.py

import pandas as pd

# =================================================================
### Function that will infer the SQL data type form pandas dtype
# =================================================================
def infer_sql_type(series: pd.Series) -> str:
    """
    Infer the appropriate PostgreSQL data type for a given pandas Series.

    This function maps the pandas data type of the input Series to a compatible
    PostgreSQL data type, following general conventions for schema inference.

    Parameters:
        series (pd.Series): A pandas Series whose data type will be mapped.

    Returns:
        str: The inferred PostgreSQL data type. One of:
            - 'INT' for integer values
            - 'FLOAT' for floating point numbers
            - 'BOOLEAN' for boolean values
            - 'TIMESTAMP' for datetime values
            - 'TEXT' for all other or mixed types

    Dependencies:
        - pandas (pd): Used for data type inference via `pd.api.types`.

    Notes:
        - `object` dtype columns (e.g., strings or mixed types) default to `TEXT`.
        - If datetime values are stored as `object`, they will not be auto-inferred
          as 'TIMESTAMP' unless explicitly cast or parsed beforehand.
    """

    if pd.api.types.is_integer_dtype(series):
        return "INT"
    elif pd.api.types.is_float_dtype(series):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    else:
        return "TEXT"

__all__ = [
    "infer_sql_type"
]