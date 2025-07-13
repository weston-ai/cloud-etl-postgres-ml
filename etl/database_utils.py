# database_utils.py

# ----- Module contains the following functions -----
# clean_column()
# infer_sql_type()
# validate_postgres_identifier()
# identify_and_split_time_invariant_columns()

import pandas as pd
import logging
from sqlalchemy import create_engine, inspect, text
import re

logger = logging.getLogger(__name__)

## Function that standardizes the column headers to a clean lowercase format
def clean_column(col, max_length=63): # the max length for column names is 63 characters in postgresql
    """
        Cleans a column name to conform with PostgreSQL naming conventions.

        Converts the name to lowercase, strips whitespace, replaces spaces,
        hyphens, and colons with underscores, and truncates to the maximum
        allowed length in PostgreSQL (default is 63 characters).

        Parameters:
            col (str): The original column name.
            max_length (int): Maximum length of the cleaned name (default: 63).

        Returns:
            str: A cleaned and truncated column name.
        """

    cleaned = (
        col.strip()
           .lower()
           .replace(" ", "_")
           .replace("-", "_")
           .replace(":", "_")
    )
    return cleaned[:max_length]

# Function that will infer the SQL data type form pandas dtype
def infer_sql_type(series):
    """
        Infers the appropriate PostgreSQL data type for a given pandas Series.

        Analyzes the pandas data type of the input Series and returns a corresponding
        SQL type string compatible with PostgreSQL.

        Parameters:
            series (pd.Series): The pandas Series whose type will be inferred.

        Returns:
            str: The inferred PostgreSQL type as a string (e.g., 'INT', 'FLOAT', 'BOOLEAN', 'TIMESTAMP', or 'TEXT').
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

# Function that validates a list of strings as safe SQL identifiers for a Postgres table
def validate_postgres_identifier(names):
    """
    Validates a list of strings as safe SQL identifiers according to PostgreSQL naming conventions.

    Each identifier must:
        - Be a string
        - Start with a lowercase letter or underscore
        - Contain only lowercase letters, digits, or underscores
        - Be at most 63 characters

    Logs the result of each individual validation.
    If any identifier is invalid, raises an exception after all checks are complete.

    Parameters:
        names (Iterable[str]): A list or iterable of SQL identifier strings.

    Returns:
        List[str]: The list of validated identifiers if all pass.

    Raises:
        ValueError: If one or more identifiers are invalid. The error lists all failed identifiers.
    """

    pattern = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
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

# Function to identify time-invariant and time-variant columns in the table. This creates a new table for each within the PostgreSQL database.
def identify_and_split_time_invariant_columns(db_engine, table_name, unique_id, database, db_inspector, error_tolerance):
    """
    Identifies time-invariant and time-variant columns in a longitudinal PostgreSQL table,
    then creates and saves two separate tables for each category.

    A column is considered time-invariant if, for at least (1 - error_tolerance) fraction of unique entities
    (e.g., patients), it contains only one unique value. Time-variant columns are all others.

    Parameters:
        db_engine (sqlalchemy.engine.Engine): SQLAlchemy engine connected to the target PostgreSQL database.
        table_name (str): Name of the original PostgreSQL table containing longitudinal data.
        unique_id (str): Name of the column uniquely identifying each entity (e.g., patient ID).
        database (str): Name of the PostgreSQL database (used for logging only).
        error_tolerance (float): Fraction (0–1) of entities allowed to violate invariance for a column
                                 to still be considered time-invariant.

    Returns:
        tuple[list[str], list[str]]:
            - time_invariant_cols: List of column names identified as time-invariant.
            - time_variant_cols: List of column names identified as time-variant.

    Notes:
        - Performs defensive checks on all identifiers to prevent SQL injection.
        - Creates two new tables:
            `{table_name}_time_invariant_sql` — one row per entity, containing invariant columns.
            `{table_name}_time_variant_sql` — all rows, containing variant columns.
        - Uses `DISTINCT ON` PostgreSQL syntax for deduplication.
        - Assumes input table is already loaded and indexed appropriately.
        - Logs detailed information and warnings for edge cases (e.g., borderline columns).
    """

    logger.info("Starting process to identify time-invariant and time-variant columns in table '%s'.", table_name)

    # Step 1: Validate PostgreSQL safety of identifiers again
    logger.info("Validating PostgreSQL safety of identifiers again.")
    identifiers = [table_name, unique_id, database]
    validate_postgres_identifier(identifiers)
    logger.info("Finished validating safety of identifiers.")

    # Step 2: Get all column names from the postgres table
    with db_engine.connect() as conn:
        columns = db_inspector.get_columns(table_name)
        column_names = [col['name'] for col in columns if col['name'] != unique_id]

    # Step 3: Validate PostgreSQL safety of column names (extra check to guard against unexpected behavior)
    logger.info("Validating PostgreSQL safety of column names.")
    validate_postgres_identifier(column_names)
    logger.info("Finished validating safety of column names.")

    # Step 4: Determine the time-invariant columns (using a threshold of variance logic of 0.01%)
    time_invariant_cols = []

    with db_engine.connect() as conn:
        for col in column_names:
            sql = f"""
                SELECT COUNT(*) AS outlier_count
                FROM (
                    SELECT {unique_id}
                    FROM {table_name}
                    GROUP BY {unique_id}
                    HAVING COUNT(DISTINCT "{col}") > 1
                ) AS sub;
            """
            try:
                outlier_count = conn.execute(text(sql)).scalar()
                total_entities = conn.execute(text(f"SELECT COUNT(DISTINCT {unique_id}) FROM {table_name}")).scalar()
                percent_outlier = outlier_count / total_entities  # calculates percentage of patients who are outliers for the column

                if percent_outlier <= error_tolerance:  # i.e. if <= 1% of patients are outliers
                    time_invariant_cols.append(col) # add "time-invariant" column to list
                    if percent_outlier > 0: # identifies patients who have variance but less than error tolerance
                        logger.warning(
                            "Column %s has %.2f%% of entities with >1 unique label, below tolerance. Marked as time-invariant.", col, percent_outlier * 100) # times 100 to get percent

            except Exception as e:
                logger.error("Error analyzing column '%s': %s", col, e, exc_info=True)

    logger.info("Identified %d time-invariant columns: %s", len(time_invariant_cols), time_invariant_cols)

    # Step 5: Determine time-variant columns
    time_variant_cols = [col for col in column_names if col not in time_invariant_cols]

    # Step 6: Create time-invariant table
    if time_invariant_cols:
        invariant_cols_str = ", ".join([f'"{col}"' for col in time_invariant_cols]) # joins the quoted columns into a single string separated by commas
        sql_invariant = f"""
            CREATE TABLE IF NOT EXISTS {table_name}_time_invariant AS  -- create table using these criteria
            -- select only one distinct observation per patient in the time-invariant data
            SELECT DISTINCT ON ("{unique_id}") "{unique_id}", {invariant_cols_str}  -- "DISTINCT ON" == Postgres
            FROM {table_name}
            ORDER BY "{unique_id}";
        """ # We cannot parameterize identifiers, which is why we used validate_postgres_identifier() earlier
        try:
            with db_engine.begin() as conn:
                conn.execute(text(sql_invariant))
            logger.info("Created postgres table '%s_time_invariant'.", table_name)
        except Exception as e:
            logger.error("Failed to create '%s_time_invariant' table: %s", table_name, e, exc_info=True)
    else:
        logger.warning("No time-invariant columns identified. Skipping creation of '%s_time_invariant' table in '%s' database.", table_name, database)

    # Step 7: Create time-variant table
    if time_variant_cols:
        variant_cols_str = ", ".join([f'"{col}"' for col in time_variant_cols]) # joins the quoted columns into a single string separated by commas
        sql_variant = f"""
            CREATE TABLE IF NOT EXISTS {table_name}_time_variant AS
            SELECT {unique_id}, {variant_cols_str}
            FROM {table_name};
        """
        try:
            with db_engine.begin() as conn:
                conn.execute(text(sql_variant))
            logger.info("Created postgres table '%s_time_variant'.", table_name)
        except Exception as e:
            logger.error("Failed to create '%s_time_variant' table: %s", table_name, e, exc_info=True)
    else:
        logger.warning("No time-variant columns identified. Skipping creation of '%s_time_variant' table in '%s' database.", table_name, database)

    return time_invariant_cols, time_variant_cols

# Pandas based method for importing postgres table into pandas; then using pandas to create time-invariant
#       and time-variant tables; and finally upload those tables to the postgres database
def identify_and_split_time_invariant_columns_pandas(db_engine, table_name, unique_id, error_tolerance):
    """
    Identifies time-invariant and time-variant columns in a longitudinal table using a pandas-based method,
    then saves each category to a new table in PostgreSQL.

    A column is considered time-invariant if, for at least (1 - error_tolerance) fraction of unique entities
    (e.g., patients), it contains only one unique value. All other columns are treated as time-variant.

    Parameters:
        db_engine (sqlalchemy.Engine): SQLAlchemy engine connected to the PostgreSQL database.
        table_name (str): Name of the table to process.
        unique_id (str): Column name that uniquely identifies each entity (e.g., patient ID).
        error_tolerance (float): Fraction (0–1) of allowed violations to still classify as time-invariant.

    Returns:
        tuple[list[str], list[str]]: Lists of time-invariant and time-variant column names.
    """

    import pandas as pd

    try:
        logger.info("Loading data from table '%s' using pandas.", table_name)
        df = pd.read_sql(f"SELECT * FROM {table_name}", db_engine)
    except Exception as e:
        logger.error("Failed to load table '%s': %s", table_name, e, exc_info=True)
        raise

    time_invariant_cols = []

    try:
        logger.info(
            "Identifying time-invariant columns. Allowing %.2f%% error tolerance "
            "(i.e., allowing 1%% of patients to have >1 unique value in a column and still treat it as time-invariant).",
            error_tolerance * 100
        )

        for col in df.columns:
            if col == unique_id:
                continue  # skip unique ID column

            variant_counts = df.groupby(unique_id)[col].nunique()
            sum_outliers = (variant_counts > 1).sum()
            percent_outlier = sum_outliers / len(variant_counts)

            if percent_outlier <= error_tolerance:
                time_invariant_cols.append(col)
                if percent_outlier > 0:
                    logger.warning(
                        "Column %s has %.2f%% of patients with >1 unique label, below tolerance. Marked as time-invariant.",
                        col, percent_outlier * 100
                    )

        logger.info("Identified %d time-invariant columns: %s", len(time_invariant_cols), time_invariant_cols)

    except Exception as e:
        logger.error("Failed to identify time-invariant columns: %s", e, exc_info=True)
        raise

    # Split into invariant and variant DataFrames
    try:
        logger.info("Creating time-invariant and time-variant DataFrames.")

        df_invariant = df[[unique_id] + time_invariant_cols].drop_duplicates(subset=unique_id)
        variant_cols = [col for col in df.columns if col not in time_invariant_cols]
        df_variant = df[variant_cols]

        logger.info("Created DataFrames. Invariant shape: %s, Variant shape: %s", df_invariant.shape, df_variant.shape)

    except Exception as e:
        logger.error("Failed to create split DataFrames: %s", e, exc_info=True)
        raise

    # Save to database
    try:
        logger.info("Saving time-invariant and time-variant DataFrames to PostgreSQL.")

        df_invariant.to_sql(f"{table_name}_time_invariant_pandas", db_engine, index=False, if_exists="replace")
        df_variant.to_sql(f"{table_name}_time_variant_pandas", db_engine, index=False, if_exists="replace")

        logger.info("Successfully saved both tables to PostgreSQL.")

    except Exception as e:
        logger.error("Failed to save tables to database: %s", e, exc_info=True)
        raise

    return time_invariant_cols, variant_cols