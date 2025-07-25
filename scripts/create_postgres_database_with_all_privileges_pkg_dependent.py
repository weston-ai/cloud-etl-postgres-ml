# create_postgres_database_with_all_privileges_pkg_dependent.py

"""
Script Name: create_postgres_database_with_all_privileges_pkg_dependent.py

Description:
    A production-ready script that programmatically provisions a new PostgreSQL database,
    assigns user privileges, writes the new connection URL to a `.env` file, and verifies
    successful creation. Designed for cross-platform use in environments like JupyterLab,
    WSL2, and Linux terminals. Built with SQLAlchemy (raw SQL execution) and custom utility modules.

Key Features:
    - Resolves project root dynamically for portable path handling
    - Loads environment variables from a project-level `.env` file
    - Validates identifiers for PostgreSQL compatibility
    - Creates a PostgreSQL database using SQLAlchemy and psycopg2
    - Grants ownership and full privileges to a specified user
    - Auto-generates and appends the new DB URL to the `.env` file
    - Verifies database creation by connecting and listing all databases
    - Logs all steps to both the console and a rotating log file

Execution Context:
    - Supports execution via JupyterLab, WSL2, or Linux terminal
    - Can be modularly integrated into Airflow DAGs, cron jobs, or CI/CD workflows

Requirements:
    - Must install weston_utils package into programming environment if not already installed
        -e.g. run "pip install ." at the same level as pyproject.toml for weston_utils (dev branch)
    - PostgreSQL user must have CREATEDB privileges
    - `.env` must contain a valid SQLAlchemy-compatible `PG_POSTGRES_URL`

Environment Variables:
    PG_POSTGRES_URL       : Required. Connection string to the system-level Postgres instance
    PG_<DBNAME>_URL       : Auto-generated. New DB connection string added to `.env`

Manually Defined Script Parameters:
    log_filename          : Name of the log file (no `.log` extension)
    log_folder_name       : Directory where logs will be stored
    LOG_LEVEL             : Logging verbosity level (e.g., logging.INFO)
    new_database_name     : Name of the new database to create
    db_owner              : PostgreSQL user who will own the new database

Logging:
    - Configured via `utils/logging_utils.py`
    - Writes log file to a subdirectory alongside the script
    - Logs include step-by-step progress and detailed error traces

Typical Usage:
    1. Ensure `.env` contains a valid `PG_POSTGRES_URL`
    2. Optionally edit `new_database_name` and `db_owner` in the script
    3. Run the script:
           $ python create_postgres_database_with_all_privileges_pkg_dependent.py
    4. Inspect logs in `log_create_database_with_all_privileges.log` for results

Author:
    Chris Weston

Last Updated:
    May 10, 2025
"""

# ==================================================
### IMPORT MODULES
# ==================================================
# System
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Logging
import logging
from weston_utils.logging_utils import configure_logging

# Database
from weston_utils.postgres_utils import create_postgres_database, validate_postgres_general_identifier, write_database_url_to_env
from sqlalchemy import create_engine, text

# File management
import pandas as pd
pd.set_option('display.max_columns', None)

# ======================================
### Resolve project root
# ======================================
# Only run this individual code block when testing this script from REPL (i.e. calculate "__file__" and "BASE_DIR"). Then skip the next code block (i.e. skip "Dynamically define the base directory"). BIG NOTE: you must comment out this code block (i.e. "Only run this individual code block when...") before you run this entire script as a .py file from bash.

'''
__file__ = "/home/cweston1/miniconda3/envs/PythonProject/projects_prod_testing/cloud_etl_postgres_ml/dev/database_mgmt/create_postgres_database_with_all_privileges_pkg_dependent.py"

BASE_DIR = Path(__file__).resolve().parent.parent
'''

# Dynamically define the base directory (only utilized when executing this script as a .py file from bash)
try:
    BASE_DIR = Path(__file__).resolve().parent.parent   # provides clarification for PyCharm (this will point the base dir to one folder upstream of the script; that's what ".parent.parent" does).
except NameError:   # This will kick in when running from JupyterLab
    BASE_DIR = Path().resolve()  # Defines the base dir as the path of the project root relative to the location of the .py script you're running.

# Ensure root directory is in sys.path based on how we defined BASE_DIR
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

# Load environment variables from .env file
load_dotenv(dotenv_path=BASE_DIR / '.env')

# ================================================
### MANUALLY DECLARED VARIABLES
# ================================================
log_filename = "log_create_database_with_all_privileges.log"
log_folder_name = "logs"
LOG_LEVEL = logging.INFO
POSTGRES_URL = "PG_POSTGRES_URL"  # this is the postgres URL name in .env
new_database_name = "diabetes_db"
db_owner = "cweston1"

# ==================================================================================
### LOGGING SETUP
# ==================================================================================
# For manual testing; only run this individual code block when conducting manual REPL testing. Skip the next code block (i.e. skip "Dynamically define filepath") if you are conducting manual REPL testing. BIG NOTE: you must comment out this code block (i.e. "For manual testing; only run...") before you execute this entire script as a .py file from bash.
'''
 log_filepath = os.path.join(Path(__file__).parent, "logs")
'''


# Dynamically define filepath (only utilized when executing this script as a .py file from bash)
try:
    log_filepath = Path(__file__).resolve().parent / log_folder_name
except NameError:
    log_filepath = Path().resolve() / log_folder_name

logger = configure_logging(log_filepath, log_filename, LOG_LEVEL) # or DEBUG, CRITICAL

# Head logs
script_objective = f"1) To create a PostgreSQL database called {new_database_name}, 2) assign user privileges, 3) add the database URL to the .env file, and 4) validate by connecting to the new database and listing ALL databases in the postgres cluster."
logger.info(f"SCRIPT OBJECTIVE: {script_objective}")

# =======================================================================
### VALIDATE THE SAFETY OF MANUALLY-DEFINED IDENTIFIERS
# =======================================================================
identifiers = [POSTGRES_URL, new_database_name, db_owner, log_folder_name, Path(log_filename).stem]
validate_postgres_general_identifier(identifiers)

# ======================================================================================
### CREATE DATABASE WITH PRIVILEGES via create_database_with_privileges() + RAW SQL
# ======================================================================================
if __name__ == "__main__":  # "if __name__ == "__main__" must be commented out during manual REPL testing
    # Retrieve Postgres URL from the environment
    POSTGRES_CONN_URL = os.getenv(POSTGRES_URL)

    if POSTGRES_CONN_URL is None:
        logger.error("Environment variable PG_POSTGRES_URL not set. Aborting.")

    else:
        logger.info("Attempting to create %s database.", new_database_name)

        # CREATE THE NEW DATABASE (note: the fxn assigns all privileges to the owner)
        create_postgres_database(
            new_database_name,
            owner=db_owner,
            conn_url=POSTGRES_CONN_URL
        )

        try:

            # ADD THE DATABASE URL TO THE .ENV FILE
            ENV_PATH = BASE_DIR / ".env"  # define the path to the .env file
            new_env_var = f"PG_{new_database_name.upper()}_URL"   # dynamically create the db env variable

            logger.info("Attempting to add database URL to the .env file.")

            write_database_url_to_env(
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