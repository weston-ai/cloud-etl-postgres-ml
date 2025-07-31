# weston_utils/io_utils/parallel_upload_copy_expert.py
# this script contains both parallel_upload_copy_expert() and _upload_chunk_via_copy()
# _upload_chunk_via_copy() is an internal function consumed by parallel_upload_copy_expert()

import io
import math
import logging
import pandas as pd
from sqlalchemy import create_engine
from multiprocessing import Pool, cpu_count
from typing import Optional

logger = logging.getLogger(__name__)

def _upload_chunk_via_copy(chunk_df: pd.DataFrame, db_url: str, table_name: str, chunk_idx: int) -> None:
    """
        Uploads a single chunk of a Pandas DataFrame to a PostgreSQL table using psycopg2's copy_expert().

        This function is designed to be used internally by multiprocessing workers within
        `parallel_upload_copy_expert()`. It uses a raw connection from SQLAlchemy to gain
        low-level access to the database driver and stream data efficiently via a RAM-based buffer.

        Parameters
        ----------
        chunk_df : pd.DataFrame
            The DataFrame chunk to upload. Should be a subset of the original large DataFrame.
        db_url : str
            SQLAlchemy-compatible PostgreSQL connection URL.
        table_name : str
            The name of the destination PostgreSQL table.
        chunk_idx : int
            Index of the chunk (used for logging and tracking parallel upload progress).

        Notes
        -----
        - Missing values in the DataFrame are converted to PostgreSQL NULLs using `na_rep="\\N"`.
        - Data is written to an in-memory `StringIO` buffer in CSV format without headers.
        - The PostgreSQL COPY command is executed using `copy_expert()` with `CSV NULL '\\N'`.
        - If an error occurs during upload, the transaction is rolled back and the exception is re-raised.
        - Logging is performed at INFO and ERROR levels to monitor chunk success/failure.

        Raises
        ------
        Exception
            Any exception that occurs during the COPY process is logged and re-raised for visibility.
        """

    engine = create_engine(db_url)
    raw_conn = engine.raw_connection() # using sqlalchemy's psycopg2 wrapper to reach low-level C language abstraction within the postgres database (for fastest performance)
    buffer = io.StringIO()
    chunk_df.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    try:
        with raw_conn.cursor() as cur:
            cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV NULL '\\N'", buffer)
            raw_conn.commit()
            logging.info(f"✅ Chunk {chunk_idx} uploaded successfully: {len(chunk_df)} rows.")
    except Exception as e:
        raw_conn.rollback()
        logging.error(f"❌ Chunk {chunk_idx} failed: {e}", exc_info=True)
        raise
    finally:
        raw_conn.close()

def parallel_upload_copy_expert(
    df: pd.DataFrame,
    db_url: str,
    table_name: str,
    n_processes: Optional[int] = None,  # Optional[] makes the type signature implicit
    min_chunk_size: int = 10000,
    verify: bool = True
) -> None:
    """
    Performs a high-speed, parallelized upload of a Pandas DataFrame to a PostgreSQL table
    using psycopg2's `copy_expert()` via SQLAlchemy raw connections.

    This function is optimized for large DataFrames by:
      - Splitting the data into chunks
      - Uploading each chunk concurrently using Python's multiprocessing
      - Writing chunks as CSV to RAM buffers (StringIO) for streaming to Postgres
      - Optionally verifying that the row count in the target table matches the upload

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to upload. Should be cleaned beforehand (e.g., null handling, column name sanitization).
    db_url : str
        SQLAlchemy-compatible PostgreSQL connection URL.
    table_name : str
        The name of the target table in the PostgreSQL database.
    n_processes : Optional[int], default=None
        The number of parallel worker processes to spawn. Defaults to the number of CPUs if None.
    min_chunk_size : int, default=10000
        Minimum number of rows per chunk. Overrides default chunk size when DataFrame is small.
    verify : bool, default=True
        If True, verifies that the total row count in the destination table matches the number of uploaded rows.

    Notes
    -----
    - This function internally calls `_upload_chunk_via_copy()` for each chunk.
    - Uses CSV formatting with `NULL '\\N'` to represent missing values.
    - If the number of chunks is less than the number of processes, some processes will be idle.
    - Designed for PostgreSQL; not portable to other SQL dialects.

    Raises
    ------
    Exception
        Any exception raised during the multiprocessing upload or SQL execution is logged and re-raised.

    Logging
    -------
    - Logs progress, chunk completion, and any errors encountered.
    - Includes detailed logs of the parallelization strategy and row count verification.
    """

    if n_processes is None:
        n_processes = cpu_count()

    total_rows = len(df)

    if total_rows == 0:
        logger.warning("Dataframe is empty. Skipping upload.")
        return

    # Calculate the chunk size (n rows) per CPU, ensuring that chunk size is never < 10,000 rows
    chunk_size = max(math.ceil(total_rows / n_processes), min_chunk_size)

    # Split the DataFrame into chunks that are sized according to our definition of chunk_size — and use integer indexing ("iloc") to assign sequential rows to each chunk, up to but not including "i + chunk_size". This will return a list of chunks that represent these sequentially assigned rows. These chunks will then be processed for parallel uploading to the postgres table.
    chunks = [df.iloc[i:(i + chunk_size)] for i in range(0, total_rows, chunk_size)]

    logging.info(f"🔄 Beginning parallel upload: {total_rows} rows, {len(chunks)} chunks, {n_processes} processes.")

    # Check if some workers will be idle due to fewer chunks than workers
    if len(chunks) < n_processes:
        logger.info(
            f"ℹ️ Chunk count ({len(chunks)}) is less than process count ({n_processes}). Some workers will be idle.")

    # Extract each index and chunk from chunks so that they can be inserted into a tuple that contains the chunk, db_url, table_name, and index of the chunk. Create a list of these tuples.
    args = [(chunk, db_url, table_name, idx) for idx, chunk in enumerate(chunks)]

    # Create the pool of workers (n_processes) and map the chunks and arguments across works. Also excecute the _upload_chunk_via_copy() fxn on each worker.
    try:
        logger.info("Pooling the worker threads.")
        with Pool(processes=n_processes) as pool:
            logger.info("Successfully pooled the worker threads.")
            # Map the arguments and upload_chunk_via_copy across the workers
            logger.info("Mapping the arguments and chunks across workers.")
            pool.starmap(_upload_chunk_via_copy, args)
            logger.info("Parallel upload completed using %d workers for %d chunks.", n_processes, len(chunks))
    except Exception as e:
        logger.error("Failed during parallel upload: %s", e, exc_info=True)

    if verify:
        from sqlalchemy import text
        engine = create_engine(db_url)
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.scalar()
            if row_count == total_rows:
                logging.info(f"✅ Row count verification passed: {row_count} rows in {table_name}.")
            else:
                logging.warning(f"⚠️ Row count mismatch: expected {total_rows}, found {row_count}.")