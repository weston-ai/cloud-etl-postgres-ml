# weston\_utils

A general-purpose Python utility package for reproducible workflows, database management, logging, and data engineering.

This package is a work in progress and will expand to include modular tools across SQL, logging, analytics, and ETL pipelines.

---

## 📦 Features

### `postgres_utils`
- Create PostgreSQL databases using SQLAlchemy
- Grant privileges and manage connections
- Validate safe SQL identifiers and clean column names
- Identify and partition time-invariant vs. time-variant columns (SQL and pandas approaches)

### `logging_utils`
- Centralized logging configuration for scripts and packages
- Support for modular logger initialization with custom levels and formats

### `io_utils`
- Parallel importing of CSV files (using Dask multithreading)
- Parallel uploading of dataframe to postgres table (using Psycopg2 & Multiprocessing)
- Serial uploading of dataframes to postgres table (choosing RAM buffer or hard-disk)

(Additional modules coming soon...)

---

## 📁 Package Layout

```text
weston_utils/                     ← Top-level folder
├── pyproject.toml                ← Build and install metadata
├── README.md                     ← Package description
├── weston_utils/                 ← Package
│   ├── __init__.py               ← Activation (enables "from weston_utils import logging_utils .... OR postgres_utils")
│   ├── logging_utils/            ← Subpackage for logging utility modules
│   │   ├── __init__.py           ← Activation 
│   │   └── configure_logging.py  ← Module (example) 
│   ├── postgres_utils/           ← Subpackage for etl and database management
│   │   ├── __init__.py           ← Activation 
│   │   ├── clean_column.py       ← Module (example)
│   ├── io_utils/                 ← Subpackage for file importing and file uploading
│   │   ├── __init__.py           ← Activation 
│   │   ├── smart_csv_loader.py   ← Module (example)
```

---

## Configuring ```__init__.py``` in each utility folder (e.g. in postgres\_utils/)
```text
from .clean_column import clean_column
from .infer_sql_type import infer_sql_type
from .create_postgres_database import create_postgres_database

__all__ = [
	"clean_column",
	"infer_sql_type",
	"create_postgres_database"
	]

**NOTE**: Inside of scripts, use "from weston_utils.postgres_utils import clean_column"
```
---

## 🚀 Installation

From your local repo:
```bash
pip install .
```

OR, as an editable development package:
```bash
pip install -e .
```

---

## 🛠️ Usage Examples

### Example 1: Create a new PostgreSQL database with privileges

from **weston\_utils.pgsql\_db\_utils** import **create\_pg\_database\_with\_all\_privileges**

```text
create_pg_database_with_all_privileges(
    dbname="analytics_db",
    owner="analytics_user",
    conn_url=os.getenv("PG_POSTGRES_URL"),
    template="template1",
    encoding="UTF8"
)
```

### Example 2: Initialize a modular logger

from **weston\_utils.logging\_utils** import **configure\_logging**

```text
configure_logging(
        log_dir=local/path/to/logs,
        log_file=filename_you_want.log,
        level=logging.INFO
)

```
logger.info("ETL job initiated.")

---

## 📦 Dependencies

- Python 3.7+
- pandas >= 2.2.3
- sqlalchemy >= 2.0.41
- python-dotenv >= 1.1.0
- psycopg2-binary >= 2.8.6

---

## 🔗 Repository
- Main Repository - https://github.com/weston-ai/cloud-etl-postgres-ml
- Dev Branch - weston\_utils

---

## 🧪 Development Roadmap

- PostgreSQL schema tools
- Environment-variable-driven config writing
- Modular logging utilities
- File I/O utilities
- Streamlined ETL logging templates
- Unit tests and CI integration

---

## 📄 License

MIT License (allows open re-use; just cite the creator)
- Chris Weston - Python utility functions for ETL pipelines
