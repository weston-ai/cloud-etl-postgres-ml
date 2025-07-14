"""
Script Name: create_postgres_database_sqlalchemy.py

Description:
    This script creates a new PostgreSQL database and optionally assigns privileges
    to a specified user using SQLAlchemy with raw SQL execution. It is designed for
    production-level workflows that require environment-driven configuration and
    connection handling, without using ORM features.

Core Functionality:
    - Connect to a PostgreSQL system database (e.g., 'postgres')
    - Issue a CREATE DATABASE command with a specified template and encoding
    - Optionally assign ownership and grant CONNECT and TEMP privileges to a user
    - Verify the creation by listing non-template databases in the cluster

Requirements:
    - The executing PostgreSQL user must have CREATEDB privilege
    - Environment variable `PG_POSTGRES_URL` must be set (used to create the new DB)
    - Optional: `PG_HEALTHDB_URL` to verify that the new database is visible in the cluster

Environment Variables:
    PG_POSTGRES_URL    : SQLAlchemy connection string to the 'postgres' system database
                         e.g., "postgresql+psycopg2://postgres:password@localhost:5432/postgres"
    PG_HEALTHDB_URL     : SQLAlchemy connection string to the newly created database for verification
                         e.g., "postgresql+psycopg2://user:password@localhost:5432/healthdb"

Logging:
    All output is logged to both console and file:
    logs/healthdb_setup.log

Usage:
    - Set required environment variables
    - Run this script directly from the command line or schedule it via cron/airflow
    - Logs will track all actions and errors during execution

Author:
    Chris Weston

Last Updated:
    May 10th, 2025
"""

import pandas as pd
pd.set_option('display.max_columns', None)
import logging
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ==================================================================================
### LOGGING SETUP
# ==================================================================================
log_format = "%(asctime)s - %(levelname)s - %(message)s"
log_filepath = "py_scripts/database/postgres_sqlalchemy/logs/healthdb_setup.log"
logging.basicConfig(
    level=logging.DEBUG,
    format=log_format,
    handlers=[
        logging.FileHandler(log_filepath),
        logging.StreamHandler()
    ]
)

logging.info(f"These logs are saved in '{log_filepath}'")
script_objective = "To create a PostgreSQL database and assign user privileges"
logging.info(f"SCRIPT OBJECTIVE: {script_objective}")

# ======================================================================================
### FUNCTION: CREATE_DATABASE_WITH_PRIVILEGES USING SQLALCHEMY + RAW SQL
# ======================================================================================
def create_database_with_privileges(dbname, owner=None, template="template1", encoding="UTF8", conn_url=None):
    """
    Create a new PostgreSQL database using SQLAlchemy (raw SQL).
    """
    if conn_url is None:
        raise ValueError("A SQLAlchemy connection URL is required.")

    try:
        logging.info(f"Connecting to system database to create '{dbname}'...")
        engine = create_engine(conn_url, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            # Build base CREATE DATABASE SQL
            create_stmt = f"CREATE DATABASE {dbname} WITH TEMPLATE {template} ENCODING '{encoding}'"
            if owner:
                create_stmt += f" OWNER {owner}"

            conn.execute(text(create_stmt))
            logging.info(f"Database '{dbname}' created successfully.")

        if owner:
            logging.info(f"Connecting to new database '{dbname}' to grant privileges to '{owner}'...")
            grant_url = conn_url.rsplit("/", 1)[0] + f"/{dbname}"
            grant_engine = create_engine(grant_url, isolation_level="AUTOCOMMIT")

            with grant_engine.connect() as conn:
                grant_stmt = f"GRANT CONNECT, TEMP ON DATABASE {dbname} TO {owner};"
                conn.execute(text(grant_stmt))
                logging.info(f"Granted CONNECT and TEMP privileges on '{dbname}' to '{owner}'.")

    except SQLAlchemyError as e:
        logging.error(f"Error creating database or granting privileges: '{dbname}': {e}", exc_info=True)

    finally:
        engine.dispose()
        if owner:
            grant_engine.dispose()
        logging.debug("All SQLAlchemy connections disposed.")

# ======================================================================================
### RUN SCRIPT / CREATE DATABASE
# ======================================================================================
# SQLAlchemy connection URL to the *postgres* system database
POSTGRES_CONN_URL = os.getenv("PG_POSTGRES_URL")

if POSTGRES_CONN_URL is None:
    logging.error("Environment variable PG_POSTGRES_URL not set. Aborting.")
else:
    create_database_with_privileges("healthdb", owner="cweston1", conn_url=POSTGRES_CONN_URL)

# ======================================================================================
### VERIFY DATABASE EXISTS IN CLUSTER
# ======================================================================================
# Import environment
HEALTHDB_CONN_URL = os.getenv("PG_HEALTHDB_URL")

# Verify database exists
if HEALTHDB_CONN_URL:
    engine = create_engine(HEALTHDB_CONN_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT datname FROM pg_database WHERE datistemplate = false;"))
        for row in result:
            print("Database:", row[0])
    engine.dispose()
else:
    logging.warning("PG_HEALTH_CONN_URL environment variable not set; skipping verification.")