# utils/pg_database_utils/write_database_url_to_env.py

from pathlib import Path
from sqlalchemy.engine.url import make_url
import os
import logging
from dotenv import load_dotenv  # used in the main script, although not used here

logger = logging.getLogger(__name__)

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
    # from pathlib import Path
    # from sqlalchemy.engine.url import make_url
    # import os

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

        logger.info("Successfully added the database URL for %s at %s", new_db_name, env_path)
    except Exception as e:
        logger.error("Failed to add the database URL for %s at %s", new_db_name, env_path, exc_info=True)

__all__ = [
    "write_database_url_to_env"
]