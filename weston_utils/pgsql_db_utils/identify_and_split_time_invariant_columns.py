# utils/pg_database_utils/identify_and_split_time_invariant_columns.py

from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

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

__all__ = [
    "identify_and_split_time_invariant_columns"
]