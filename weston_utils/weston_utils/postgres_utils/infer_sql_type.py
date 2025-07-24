# utils/postgres_utils/infer_sql_type.py

import pandas as pd
import logging

logger =logging.getLogger(__name__) # Creates a module level logger

POSTGRES_SQL_TYPES = {
    "INT", "FLOAT", "BOOLEAN", "TIMESTAMP", "INTERVAL", "TEXT", "VARCHAR"
}

# =================================================================
### Function that will infer the SQL data type from pandas dtype
# =================================================================
def infer_sql_type(series: pd.Series, prefer_varchar: bool = False) -> str:
    """
    Infer the most appropriate PostgreSQL data type for a given pandas Series.

    This function maps the pandas dtype of a column to a compatible PostgreSQL type
    using heuristics and sample-based inspection when needed. It covers common
    numeric, boolean, timestamp, and text types, including special handling of
    ambiguous string columns.

    Parameters:
        series (pd.Series):
            The pandas Series to inspect.
        prefer_varchar (bool, optional):
            If True, use 'VARCHAR' instead of 'TEXT' for string columns.
            Defaults to False.

    Returns:
        str:
            The inferred PostgreSQL column type. One of:
                - 'INT' for integer types
                - 'FLOAT' for float types
                - 'BOOLEAN' for bools
                - 'TIMESTAMP' for datetime64 types
                - 'INTERVAL' for timedelta64 types
                - 'VARCHAR' or 'TEXT' for string-like or fallback types

    Logging:
        - If a string column appears to contain mostly datetime-formatted values
          but is not explicitly typed as datetime, a warning is logged.

    Notes:
        - Only the first 10 non-null values are inspected for datetime-like strings.
        - Pandas is_string_dtype and object columns default to TEXT/VARCHAR.
        - Date parsing is handled via `pd.to_datetime()` and may trigger false positives
          on short numeric strings or ambiguous formats (e.g., "07/11").

    Example:
        " >>> infer_sql_type(pd.Series([1, 2, 3]))
        'INT'

        " >>> infer_sql_type(pd.Series(["2023-01-01", "2023-01-02"]))
        'TEXT'  # But logs a warning that these appear datetime-like

    Dependencies:
        - pandas (pd)
        - logging (for warning output)

    Python Compatibility:
        - Compatible with Python 3.6+
        - Developed and tested with Python 3.11.11
    """

    dtype = series.dtype
    colname = series.name or "<unnamed>"   # causes colname to be "unnamed" if actually blank

    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    elif pd.api.types.is_timedelta64_dtype(dtype):
        return "INTERVAL"
    elif isinstance(dtype, pd.CategoricalDtype):
        return "VARCHAR" if prefer_varchar else "TEXT"
    elif pd.api.types.is_string_dtype(dtype) or dtype == object:
        sample = series.dropna().head(10).astype(str)

        # Try to parse string as datetime and warn if looks like a date
        datetime_hits = 0
        for val in sample:
            try:
                pd.to_datetime(val)
                datetime_hits += 1  # increase datetime_hits by 1 if val is converted to a possible datetime
            except Exception:  # if pd.to_datetime() does not infer a datetime object, then skip to next value
                continue

        if datetime_hits >= 0.3 * len(sample):  # if datetime_hits is 30% of sample, then trigger warning
            logger.warning(
                "Column '%s' contains mostly strings that parse as datetimes. Consider casting to datetime.",
                colname,
            )

        return "VARCHAR" if prefer_varchar else "TEXT"
    else:
        return "TEXT"

__all__ = [
    "infer_sql_type"
]