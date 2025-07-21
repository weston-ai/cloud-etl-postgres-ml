# weston\_utils

A general-purpose Python utility package for reproducible workflows, database management, logging, and data engineering.

This package is a work in progress and will expand to include modular tools across SQL, logging, analytics, and ETL pipelines.

---

## 📦 Features

### 📊 `pgsql_db_utils`
- Create PostgreSQL databases using SQLAlchemy
- Grant privileges and manage connections
- Validate safe SQL identifiers and clean column names
- Identify and partition time-invariant vs. time-variant columns (SQL and pandas approaches)

### 📋 `logging_utils`
- Centralized logging configuration for scripts and packages
- Support for modular logger initialization with custom levels and formats

(Additional modules coming soon...)

---

## 📁 Package Layout

```text
weston_utils/                     ← Top-level folder
├── pyproject.toml                ← Build and install metadata
├── README.md                     ← Package description
├── weston_utils/                 ← Package
│   ├── __init__.py               ← Activation
│   ├── logging_utils/            ← Subpackage
│   │   ├── __init__.py           ← Activation 
│   │   └── configure_logging.py  ← Module 
│   ├── pgsql_db_utils/           ← Subpackage 
│   │   ├── __init__.py           ← Activation 
│   │   ├── clean_column.py       ← Module 
```

---

## 🚀 Installation

From your local repo:
```bash
pip install .
```

OR, as an editable development package:
```bash
pip install -e .[dev]
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
