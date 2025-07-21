# _master_pgsql_db_utils.py

# ----- Module contains the following functions -----
# create_database_with_privileges()
# write_database_url_to_env()
# validate_postgres_column_identifier()
# validate_postgres_general_identifier()
# clean_column()
# infer_sql_type()
# identify_and_split_time_invariant_columns()  # uses SQL in the database
# identify_and_split_time_invariant_columns_pandas()  # uses Pandas; assumes data loaded into RAM

import pandas as pd
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.url import make_url
from typing import Union, Iterable
import re
import os
from dotenv import load_dotenv  # used in the main script, although not used here
import psycopg2  # used in certain scripts, although not used here

logger = logging.getLogger(__name__)

# ======================================================================================
### FUNCTION: CREATE_DATABASE_WITH_PRIVILEGES USING SQLALCHEMY + RAW SQL
# ======================================================================================
def create_pg_database_with_all_privileges(
        dbname=None,
        owner=None,
        template="template1",
        encoding="UTF8",
        conn_url=None
) -> None:

    """
    Create a new PostgreSQL database using raw SQL via SQLAlchemy and optionally assign privileges to a specified user.

    This function connects to a PostgreSQL system-level database (e.g., 'postgres') and issues a
    `CREATE DATABASE` statement with the provided template and encoding. If an `owner` is specified,
    the function grants full privileges (ALL) on the new database to that user.

    Parameters:
        dbname (str):
            Name of the new PostgreSQL database to create.
        owner (str, optional):
            Username to assign as the database owner and grant privileges to. Defaults to None.
        template (str, optional):
            The template to base the new database on (e.g., 'template1'). Defaults to "template1".
        encoding (str, optional):
            Character encoding for the database. Defaults to "UTF8".
        conn_url (str):
            SQLAlchemy-compatible connection string to the system-level database (must have CREATEDB privilege).

    Raises:
        ValueError: If `conn_url` is not provided.
        SQLAlchemyError: If the database creation or privilege assignment fails.

    Logging:
        Logs all major actions including engine creation, database creation, privilege granting,
        and exception handling. Also logs resource cleanup via engine disposal.

    Example:
        create_database_with_privileges(
            dbname="analytics_db",
            owner="analytics_user",
            conn_url=os.getenv("PG_POSTGRES_URL")
        )

    Dependencies:
    - SQLAlchemy
    - psycopg2
    - Python standard libraries: logging, os

    Python Compatibility:
        - Compatible with Python 3.6+
        - Developed and tested on Python 3.11.11
    """

    # Make sure real variables are passed to function
    required_args = {
        "dbname": dbname,
        "owner": owner,
        "conn_url": conn_url
    }

    for name, value in required_args.items():
        if value is None:
            raise ValueError(f"Missing required argument: '{name}' cannot be None.")

    # Make sure connection URL exists
    if conn_url is None:
        raise ValueError("A SQLAlchemy connection URL is required.")

    grant_engine = None  # defined here to prevent UnboundLocalError

    try:
        # Create database engine activate automatic commit if the function executes fully
        logger.info("Creating database engine to connect to '%s' database.", dbname)
        engine = create_engine(conn_url, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            # Build base CREATE DATABASE SQL
            create_stmt = f"CREATE DATABASE {dbname} WITH TEMPLATE {template} ENCODING '{encoding}'"
            if owner:
                create_stmt += f" OWNER {owner}"  # appends an owner after OWNER if owner exists

            conn.execute(text(create_stmt))  # execute the SQL command
            logger.info("Successfully created '%s' database.", dbname)

        # Create new database URL using the Postgres URL as a template, and then create a database engine for the new database URL
        if owner:
            logger.info("Connecting to new database '%s' to grant privileges to '%s'.", dbname, owner)
            grant_url = conn_url.rsplit("/", 1)[0] + f"/{dbname}"  # splits the postgres URL at the last "/" and then inserts "/" + the new database name. This effectively creates the URL string for the new database.
            grant_engine = create_engine(grant_url, isolation_level="AUTOCOMMIT") # create new database engine

            # Connect to the new database and assign connection permissions to the defined owner
            with grant_engine.connect() as conn:
                grant_stmt = f"GRANT ALL PRIVILEGES ON DATABASE {dbname} TO {owner};"
                conn.execute(text(grant_stmt))
                logger.info("Granted ALL privileges on '%s' to '%s'", dbname, owner)

    # Error for failure to create database
    except SQLAlchemyError as e:
        logger.error("Error creating database or granting privileges: '%s'; %s", dbname, e, exc_info=True)
        raise

    # Kill connections to database
    finally:
        engine.dispose()
        if grant_engine:
            grant_engine.dispose()
        logger.debug("All SQLAlchemy connections disposed.")

# ======================================================================
### Function that writes the new database URL to the .env file
# ======================================================================
def write_database_url_to_env(
    source_env_var: str = "",
    new_env_var: str = "",
    new_db_name: str = "",
    env_path: str = ""
) -> None:
    """
    Generate a new SQLAlchemy database URL from an existing one and write it to a `.env` file.

    This function reads an existing environment variable (typically pointing to a system-level PostgreSQL
    database), replaces the database name with a new one, and writes the resulting connection string
    to the specified `.env` file under a new environment variable name. If the target variable already
    exists in the `.env` file, it will be overwritten.

    Parameters:
        source_env_var (str):
            Name of the existing environment variable containing the source SQLAlchemy URL.
        new_env_var (str):
            Name of the new environment variable to write (e.g., 'PG_HEALTHDB_URL').
        new_db_name (str):
            Name of the new database to be used in the generated URL.
        env_path (Path):
            Path to the `.env` file to update.

    Raises:
        ValueError: If `source_env_var` is not set in the environment.
        Exception: If file read/write operations fail.

    Behavior:
        - Parses and modifies the SQLAlchemy URL using `sqlalchemy.engine.url.make_url`
        - Updates or appends the environment variable in the `.env` file
        - Ensures proper newline formatting and safe file handling
        - Logs success or failure using the configured logger

    Example:
        " >>> write_database_url_to_env(
        " >>>     source_env_var="PG_POSTGRES_URL",
        " >>>     new_env_var="PG_HEALTHDB_URL",
        " >>>     new_db_name="healthdb",
        " >>>     env_path=Path(".env")
        " >>> )

    Dependencies:
        - SQLAlchemy
        - Python standard libraries: os, pathlib

    Python Compatibility:
        - Compatible with Python 3.6+
        - Developed and tested on Python 3.11.11
    """
    try:
        original_url = os.getenv(source_env_var)
        if not original_url:
            raise ValueError(f"Environment variable '{source_env_var}' not set.")

        # Parse the original URL and replace the database name with the new database name
        new_url = (
            make_url(original_url)
            .set(database=new_db_name)
            .render_as_string(hide_password=False)
        )

        # Read existing lines
        lines = []
        replaced = False
        if env_path.exists():
            with open(env_path, 'r') as f:
                lines = f.readlines()

            # Ensure the last line ends with a newline
            if lines and not lines[-1].endswith('\n'):
                lines[-1] += '\n'

            # Update existing key if present
            for i, line in enumerate(lines):
                if line.startswith(f"{new_env_var}="):
                    lines[i] = f'{new_env_var}="{str(new_url)}"\n'
                    replaced = True

        # Add the new entry if not replaced
        if not replaced:
            lines.append(f'{new_env_var}="{str(new_url)}"\n')

        with open(env_path, 'w') as f:
            f.writelines(lines)

        logger.info("Successfully added the database URL for '%s' at %s", new_db_name, env_path)
    except Exception as e:
        logger.error("Failed to add the database URL for '%s' at %s", new_db_name, env_path, exc_info=True)

# ============================================================================================
### Function that validates a list of strings as safe COLUMN IDENTIFIERS for a Postgres table
# ============================================================================================
def validate_postgres_column_identifier(names):
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

# ============================================================================================================
### Function to identify time-invariant and time-variant columns in the table. This creates a new table for each within the PostgreSQL database.
# ============================================================================================================
def identify_and_split_time_invariant_columns(
    db_engine=None,
    table_name: str = "",
    unique_id: str = "",
    database: str = "",
    db_inspector=None,
    error_tolerance: float = 0.01
) -> tuple[list[str], list[str]]:

    """
    Analyze a longitudinal PostgreSQL table to identify time-invariant vs. time-variant columns,
    and create two new tables reflecting this separation.

    A column is considered time-invariant if, for at least `(1 - error_tolerance)` fraction of unique
    entities (e.g., patients), it contains only one distinct value across all timepoints. All other
    columns are considered time-variant.

    Two new tables are created in PostgreSQL:
        - `{table_name}_time_invariant`: One row per unique entity, with invariant columns.
        - `{table_name}_time_variant`: All rows, with variant columns.

    Parameters:
        db_engine (sqlalchemy.engine.Engine, optional): A SQLAlchemy engine connected to the database.
                                                        Required for all database operations.
        table_name (str): Name of the source PostgreSQL table containing longitudinal data.
        unique_id (str): Column name that uniquely identifies each entity (e.g., patient ID).
        database (str): Name of the database (used for logging context only).
        db_inspector (sqlalchemy.engine.reflection.Inspector): SQLAlchemy inspector object for column introspection.
        error_tolerance (float): Allowable proportion (0–1) of entities that may violate invariance
                                 for a column to still be considered time-invariant.

    Returns:
        tuple[list[str], list[str]]:
            - List of column names identified as time-invariant.
            - List of column names identified as time-variant.

    Raises:
        ValueError: If `db_engine` is not provided.
        Exception: Logged if SQL execution fails during processing or table creation.

    Dependencies:
        - SQLAlchemy (`sqlalchemy.engine.Engine`, `sqlalchemy.sql.text`)
        - Python standard library: `logging`
        - Assumes `validate_postgres_identifier()` exists elsewhere to sanitize table/column names.

    Notes:
        - All database identifiers should be validated for SQL safety before use.
        - PostgreSQL's `DISTINCT ON` is used to efficiently extract invariant values per entity.
        - Tables are created using `CREATE TABLE IF NOT EXISTS` for safe, idempotent operations.
        - Columns with minor variance below the `error_tolerance` threshold are flagged but still treated as invariant.
    """

    if db_engine is None:
        raise ValueError("A valid SQLAlchemy database engine must be provided.")

    logger.info("Starting process to identify time-invariant and time-variant columns in table '%s'.", table_name)

    # Step 1: Get all column names from the postgres table
    with db_engine.connect() as conn:
        columns = db_inspector.get_columns(table_name)
        column_names = [col['name'] for col in columns if col['name'] != unique_id]

    # Step 2: Determine the time-invariant columns (using a threshold of variance logic of 0.01%)
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

    # Step 3: Determine time-variant columns
    time_variant_cols = [col for col in column_names if col not in time_invariant_cols]

    # Step 4: Create time-invariant table
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

    # Step 5: Create time-variant table
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

# ===========================================================================================================
### Pandas based method for importing postgres table into pandas; then using pandas to create time-invariant and time-variant tables; and finally upload those tables to the postgres database
# ===========================================================================================================
def identify_and_split_time_invariant_columns_pandas(
    db_engine=None,
    table_name: str = "",
    unique_id: str = "",
    error_tolerance: float = 0.01,
) -> tuple[list[str], list[str]]:

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

    if db_engine is None:
        raise ValueError("A valid SQLAlchemy database engine must be provided.")

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