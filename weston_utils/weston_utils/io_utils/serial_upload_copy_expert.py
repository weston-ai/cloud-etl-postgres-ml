# weston_utils/io_utils/serial_upload_copy_expert.py

# Libraries and modules
import io
import tempfile
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

def serial_upload_copy_expert(
    df: pd.DataFrame,
    db_engine: Engine,
    target_table: str,
    database: str,
    ram_buffer_limit: float = 3.0
) -> None:
    """
    Upload a DataFrame to a PostgreSQL table using psycopg2's optimized COPY command.
    Chooses between buffering to RAM or using temporary disk based on dataframe size.

    Parameters:
    -----------
    df : pd.DataFrame
        The dataframe to be uploaded.
    db_engine : sqlalchemy.Engine
        SQLAlchemy engine for the target database.
    target_table : str
        The name of the target PostgreSQL table.
    database : str
        Friendly name of the database (used in logging).
    ram_buffer_limit : float
        Threshold in GB to determine whether to use RAM or disk buffering.
    """

    # Estimate memory size of dataframe
    try:
        logger.info("Estimating the memory size of the dataframe")
        # Estimate the size of the dataframe in gigabytes
        mem_df = df.memory_usage(deep=True).sum() / 1_000_000_000
        logger.info("Dataframe has filesize = %d GB.", mem_df)
    except Exception as e:
        logger.error("Failed to estimate memory size of dataframe")
        raise

    # Get raw psycopg2 connection for low-level COPY access
    raw_conn = db_engine.raw_connection()

    try:
        with raw_conn.cursor() as cur:
            # ----------------------------------------------
            # Upload large dataframe using temporary disk
            # ----------------------------------------------
            if mem_df >= ram_buffer_limit:
                logger.info("Attempting to copy large dataframe (%d GB) to temporary file on hard-disk.", mem_df)

                # Copy large dataframe to hard-disk, and then copy to postgres table
                with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv') as tmp:
                    df.to_csv(tmp, index=False, header=False, na_rep="\\N")
                    tmp.seek(0)
                    logger.info("Successfully copied large dataframe to temporary file on hard-disk.")
                    cur.copy_expert(f"COPY {target_table} FROM STDIN WITH CSV NULL '\\N'", tmp)

                logger.info("Successfully copied large dataframe to table %s in %s database.", target_table, database)

            # ----------------------------------------------
            # Upload smaller dataframe using RAM buffer
            # ----------------------------------------------
            else:
                # Buffer small dataframe to RAM
                logger.info("Attempting to buffer small dataframe (%d GB) to RAM.", mem_df)
                buffer = io.StringIO()
                df.to_csv(buffer, index=False, header=False, na_rep="\\N")
                buffer.seek(0)
                logger.info("Successfully buffered dataframe to RAM.")

                # Copy dataframe from RAM to postgres table
                cur.copy_expert(f"COPY {target_table} FROM STDIN WITH CSV NULL '\\N'", buffer)

                logger.info("Successfully copied small dataframe to table %s in %s database using buffer.", target_table, database)

            # Commit the transaction
            raw_conn.commit()
            logger.info("Successfully committed transaction.")

    except Exception as e:
        raw_conn.rollback()
        logger.error("Failed to copy dataframe into %r table in %r database: %s", target_table, database, e, exc_info=True)
        raise

    finally:
        raw_conn.close() # close database connection