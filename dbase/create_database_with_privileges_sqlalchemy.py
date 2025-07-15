"""
Script Name: create_postgres_database_sqlalchemy.py

Description:
    A production-ready script that programmatically provisions a new PostgreSQL database,
    grants user privileges, writes the new connection URL to a `.env` file, and verifies
    successful creation. Built with SQLAlchemy (raw SQL execution) and designed to run
    in cross-platform environments including JupyterLab and WSL2.

Key Features:
    - Dynamically resolves the project root directory for robust path handling
    - Loads environment variables from a project-level `.env` file
    - Validates manually-specified identifiers for PostgreSQL compatibility
    - Creates a new PostgreSQL database using SQLAlchemy and psycopg2
    - Assigns ownership and access privileges to a specified user
    - Automatically generates and writes the new database URL to the `.env` file
    - Verifies the new database by connecting and listing databases in the cluster
    - Logs all actions to both console and a specified log file

Execution Context:
    - Works reliably in JupyterLab, WSL2, and Linux-based terminal environments
    - Designed for modular integration into Airflow pipelines, cron jobs, or CI/CD tools

Requirements:
    - PostgreSQL user must have CREATEDB privileges
    - SQLAlchemy-compatible connection string set as PG_POSTGRES_URL in `.env`

Environment Variables:
    PG_POSTGRES_URL       : Required. Connection string to the system-level Postgres database
    PG_<DBNAME>_URL       : Auto-generated. URL for the new database written into `.env`

Manually Defined Script Parameters:
    - log_filename        : Name of the log file (without .log extension)
    - log_folder_name     : Folder where logs are written
    - LOG_LEVEL           : Logging verbosity (e.g., logging.INFO)
    - new_database_name   : Name of the database to be created
    - db_owner            : Username that will own the new database

Logging:
    - Configured via `utils/logging_utils.py`
    - Log files written to a subdirectory in the script's parent path
    - Logs include step-by-step trace of the script’s execution and error stack traces

Typical Usage:
    1. Ensure `.env` contains a valid `PG_POSTGRES_URL`
    2. Optionally edit `new_database_name` and `db_owner` in the script
    3. Run the script directly:
           $ python create_postgres_database_sqlalchemy.py
    4. Inspect `log_dev_dbase/log_dbase_dev.log` for execution details

Author:
    Chris Weston

Last Updated:
    May 10, 2025
"""

# ================================================================================================
### SET BASE DIRECTORY (ensures that cwd is used as the system path when executing from JupyterLab, and it ensures that system path is set to the location of the script when executing in WSL2).
# ================================================================================================
import sys
from pathlib import Path
from dotenv import load_dotenv

### Resolve project root
# For manual testing....comment out the line below after REPL testing is completed.
# __file__ = "/home/cweston1/miniconda3/envs/PythonProject/projects/cloud_etl_postgres_ml/dbase/"

# Dynamically define the base directory
try:
    BASE_DIR = Path(__file__).resolve().parent.parent   # provides clarification for PyCharm (this will point the base dir to one folder upstream of the script; that's what ".parent.parent" does).
except NameError:   # This will kick in when running from JupyterLab
    BASE_DIR = Path().resolve()  # Defines the base dir as the path of the project root relative to the location of the .py script you're running.

# Ensure root directory is in sys.path based on how we defined BASE_DIR
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

# Load environment variables from .env file
load_dotenv(dotenv_path=BASE_DIR / '.env')

# =================================
### IMPORT UTILITY MODULES
# =================================
from utils import logging_utils, database_utils    #  configure_logging(), create_database_with_privileges()

# ====================================
### IMPORT THE REMAINING MODULES
# ====================================
import logging
import os
import pandas as pd
pd.set_option('display.max_columns', None)
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import SQLAlchemyError
from psycopg2 import sql
import re
import time

# ================================================
### MANUALLY DECLARED VARIABLES
# ================================================
log_filename = "log_dbase_dev"   # the ".log" extension is added in the function
log_folder_name = "log_dev_dbase"
LOG_LEVEL = logging.INFO
POSTGRES_URL = "PG_POSTGRES_URL"  # this is the postgres URL name in .env
new_database_name = "diabetes_db"
db_owner = "cweston1"

# ==================================================================================
### LOGGING SETUP
# ==================================================================================
## For manual testing....comment out the line below after REPL testing is completed.
# log_filepath = os.path.join(Path(__file__).resolve(), "log_dev_dbase")

# Dynamically define filepath
try:
    log_filepath = Path(__file__).resolve().parent / log_folder_name
except NameError:
    log_filepath = Path().resolve() / log_folder_name

logger = logging_utils.configure_logging(log_filepath, log_filename, LOG_LEVEL) # or DEBUG, CRITICAL

# Head logs
script_objective = f"1) To create a PostgreSQL database called {new_database_name}, 2) assign user privileges, 3) add the database URL to the .env file, and 4) validate by connecting to the new database and listing ALL databases in the postgres cluster."
logger.info(f"SCRIPT OBJECTIVE: {script_objective}")

# =======================================================================
### VALIDATE THE SAFETY OF MANUALLY-DEFINED IDENTIFIERS
# =======================================================================
identifiers = [POSTGRES_URL, new_database_name, db_owner, log_folder_name, log_filename]
database_utils.validate_postgres_general_identifier(identifiers)

# ======================================================================================
### CREATE DATABASE WITH PRIVILEGES via create_database_with_privileges() + RAW SQL
# ======================================================================================
if __name__ == "__main__":
    # Retrieve Postgres URL from the environment
    POSTGRES_CONN_URL = os.getenv(POSTGRES_URL)

    if POSTGRES_CONN_URL is None:
        logger.error("Environment variable PG_POSTGRES_URL not set. Aborting.")

    else:
        # CREATE THE NEW DATABASE AND ASSIGN OWNER PRIVILEGES
        logger.info("Attempting to create %s database.", new_database_name)

        database_utils.create_database_with_privileges(
            new_database_name,
            owner=db_owner,
            conn_url=POSTGRES_CONN_URL
        )

        try:
            # ADD THE DATABASE URL TO THE .ENV FILE
            ENV_PATH = BASE_DIR / ".env"  # define the path to the .env file
            new_env_var = f"PG_{new_database_name.upper()}_URL"   # dynamically create the db env variable

            logger.info("Attempting to add database URL to the .env file.")

            database_utils.write_database_url_to_env(
                source_env_var="PG_POSTGRES_URL",
                new_env_var=new_env_var,  # makes new URL uppercase
                new_db_name=new_database_name,
                env_path=ENV_PATH
            )
        except Exception as e:
            logger.error("Failed to add database URL to the .env file.")
            raise

# ================================================================================================
### VERIFY DATABASE EXISTS IN CLUSTER BY TRYING TO CONNECT TO THE NEW DB AND LISTING ALL DATABASES
# ================================================================================================
# Load environment variables again (necessary to get updated .env file)
try:
    logger.info("Loading the .env again.")
    load_dotenv(dotenv_path=BASE_DIR / '.env')
    logger.info("Successfully loaded .env into environment.")
except Exception as e:
    logger.error("Failed to load the .env again: %s", e, exc_info=True)
    raise

# Get URL for the newly created database
try:
    logger.info("Attempting to fetch '%s' for '%s' database.", new_env_var, new_database_name)
    DB_CONN_URL = os.getenv(new_env_var)
    logger.info("Successfully fetched URL.")
except Exception as e:
    logger.error("Failed to fetch URL: %s", e, exc_info=True)
    raise

# Verify database exists
if DB_CONN_URL:
    try:
        logger.info("Attempting to connect to '%s' database and list all databases.", new_database_name)

        # CREATE DATABASE ENGINE
        engine = create_engine(DB_CONN_URL)

        # CONNECT TO NEW DATABASE AND LIST ALL DATABASES IN THE POSTGRES CLUSTER
        with engine.connect() as conn:
            result = conn.execute(text("SELECT datname FROM pg_database WHERE datistemplate = false;"))
            for row in result:
                print("Database:", row[0])
        logger.info("Successfully connected to '%s' database and listed all databases.", new_database_name)

        engine.dispose()

    except Exception as e:
        logger.error("Failed to connect to '%s' database and list all databases.", new_database_name)
        raise
else:
    logging.warning("Environment variable for '%s' not set; skipping verification.", new_env_var)

### Finish logging
logger.info("Script finished running successfully.")