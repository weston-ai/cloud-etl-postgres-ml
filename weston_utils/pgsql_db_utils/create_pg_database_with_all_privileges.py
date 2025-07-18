# utils/pg_database_utils/create_pg_database_with_all_privileges.py

# ======================================================================================
### FUNCTION: CREATE_DATABASE_WITH_PRIVILEGES USING SQLALCHEMY + RAW SQL
# ======================================================================================
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

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
    # import logging
    # from sqlalchemy import create_engine, text
    # from sqlalchemy.exc import SQLAlchemyError

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

__all__ = [
    "create_pg_database_with_all_privileges"
]