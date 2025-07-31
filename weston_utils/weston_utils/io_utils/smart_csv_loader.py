# weston_utils/io_utils/smart_csv_loader.py
# includes both smart_csv_loader() and estimate_max_threads()
# estimate_max_threads() is used inside of smart_csv_loader()

import os
import logging
import pandas as pd
import dask.dataframe as dd
import dask
from multiprocessing.pool import ThreadPool
import time

logger = logging.getLogger(__name__)

def _estimate_max_threads(size_mb, max_threads=os.cpu_count()) -> int:
    """
    Estimate number of threads for Dask CSV loading, based on file size.
    Assumes pandas is used for files < 100 MB.
    """
    if size_mb < 100:
        return 1
    elif size_mb <= 200:
        return 2
    elif size_mb <= 400:
        return 3
    elif size_mb <= 800:
        return 4
    elif size_mb <= 1600:
        return 6
    else:
        return max_threads

def smart_csv_loader(file_path, threshold_mb=100, dask_compute=True, max_threads_cap=os.cpu_count()) -> pd.DataFrame:
    """
    Efficiently loads a CSV file into memory using either pandas or Dask depending on file size.

    This function first calculates the size of the CSV file on disk, then decides whether to load it
    using `pandas.read_csv()` or `dask.dataframe.read_csv()` based on a user-defined threshold.
    Files larger than `threshold_mb` will be read using Dask with multithreading and optionally
    converted to a pandas DataFrame using `.compute()`.

    Parameters
    ----------
    file_path : str
        Absolute or relative path to the CSV file.
    threshold_mb : int, default=100
        Threshold in megabytes for deciding whether to use pandas (below threshold) or Dask (above threshold).
    dask_compute : bool, default=True
        If True, the resulting Dask DataFrame is immediately computed and returned as a pandas DataFrame.
        If False, the function returns the Dask DataFrame as-is.
    max_threads_cap : int, default=os.cpu_count()
        Maximum number of threads allowed for Dask multithreading pool. Automatically determined by CPU count
        unless overridden.

    Returns
    -------
    pd.DataFrame or dask.dataframe.DataFrame
        The loaded data. Returns a pandas DataFrame if the file is small or if `dask_compute=True`.
        Returns a Dask DataFrame if the file is large and `dask_compute=False`.

    Raises
    ------
    Exception
        If file size cannot be determined, or if the import via pandas or Dask fails.

    Logging
    -------
    - Logs file size, tool selection logic, and total elapsed time for read operation.
    - Logs thread pool configuration for Dask.
    - Logs success or failure at every stage of import logic.
    """

    # Calculate the size of the file that we want to import
    try:
        logger.info("Calculating the size of the file at %s", file_path)
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        logger.info("Successfully calculated file size: %.0f MB", size_mb)
    except Exception as e:
        logger.error("Failed to calculate size of file at %r: %s", file_path, e, exc_info=True)
        raise

    # Evaluate whether pandas or dask should be used to import the file
    try:
        logger.info("Evaluating if pandas or dask should be used to import file (dependent on file size).")
        if size_mb < threshold_mb:
            logger.info("Pandas should be used for import. File size < threshold (%d MB).", threshold_mb)
        else:
            logger.info("Dask should be used for import. File size >= threshold (%d MB).", threshold_mb)
    except Exception as e:
        logger.error("Failed to determine whether pandas or dask should be used to import the file at %s: %s", file_path, e, exc_info=True)
        raise

    # Import file with pandas if file size is below threshold size
    if size_mb < threshold_mb:
        try:
            logger.info("Importing file with pandas (file size < %d MB)", threshold_mb)

            # Start time
            start_time = time.time()

            # Import file with pandas
            df = pd.read_csv(file_path)

            # End time
            end_time = time.time()

            # Elapsed time
            elapsed_minutes = (end_time - start_time) / 60

            logger.info("Successfully used pandas to import %s (size: %.0f MB).", file_path, size_mb)

            logger.info("Pandas import required %.3f minutes.", elapsed_minutes)
        except Exception as e:
            logger.error("Failed to import file with pandas although file size less than required threshold (< %d MB): %s", threshold_mb, e, exc_info=True)
            raise

    # Import file with dask if filesize is >= size threshold
    else:
        try:
            # Determine number of threads that will be used by dask
            logger.info("Determining thread count used to import file via dask.")
            max_threads = _estimate_max_threads(size_mb, max_threads=max_threads_cap)
            dask.config.set(pool=ThreadPool(max_threads))
            logger.info("Successfully determined that '%d' threads will be used for dask import.", max_threads)
        except Exception as e:
            logger.error("Failed to determine thread count: %s", e, exc_info=True)

        try:
            # Import file with dask, using multithreading
            logger.info("Importing data with dask multithreading.")

            # Start time
            start_time = time.time()

            # Import file with dask
            df = dd.read_csv(file_path)

            logger.info("Successfully used dask to import %s (size: %.0f MB).", file_path, size_mb)

            # Compute dask dataframe to pandas dataframe
            if dask_compute:
                logger.info("Computing dask dataframe to pandas dataframe.")
                df = df.compute()

            logger.info("Successfully computed dask dataFrame to pandas dataframe.")

            # End time
            end_time = time.time()

            # Elapsed time
            elapsed_minutes = (end_time - start_time) / 60

            logger.info("Dask import required %.3f minutes.", elapsed_minutes)

        except Exception as e:
            logger.error("Failed to import file with dask although file size was above threshold to use dask for import (> %d MB): %s", threshold_mb, e, exc_info=True)

    return df