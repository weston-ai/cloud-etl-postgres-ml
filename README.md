# 🌐 cloud-etl-postgres-ml

This project showcases a **modular, cloud-native ETL and analytics pipeline** using Google Drive, DuckDB, and Supabase (PostgreSQL) — all orchestrated from Colab and GitHub.

## 📌 Project Goals

- Ingest raw data from Google Drive; clean and normalize tables for Postgres structure
- Filter and transform tables via DuckDB SQL in Google Colab, using a use-case query logic 
- Load structured tables into a Supabase-hosted PostgreSQL database  
- Maintain a portable, lightweight data science stack from anywhere
- Use PyCharm and JupyterLab for heavy dev-op testing (modularity, scalability, visualization, stats, and ML)

---

## 🧠 System Architecture & Tool Roles

This project emphasizes a **modular, SQL-centric, cloud-native analytics pipeline** — blending Python, SQL, statistics, and machine learning workflows with a lightweight, mobile-friendly infrastructure.

### 🔧 Programming + Orchestration

- **Python** — The central scripting language for data loading, transformation, validation, and automation.  
- **SQLAlchemy** — Enables raw SQL execution and manages secure PostgreSQL connections from Python.  
- **psycopg2** — High-performance PostgreSQL adapter used by SQLAlchemy under the hood.  
- **pandas** — Handles data manipulation, file I/O, and schema validation. Also bridges CSVs and database tables.

### 📦 Lightweight SQL Analytics

- **DuckDB** — An embedded SQL engine for querying CSVs directly. Perfect for fast prototyping and EDA in Colab.  
- **Google Colab + Jupyter Notebooks** — Used for interactive SQL queries, cleaning, transformation, and EDA — no local setup needed.

### 🗄️ Cloud Data Infrastructure

- **Supabase (PostgreSQL)** — Secure cloud-hosted PostgreSQL backend for storing cleaned, validated production datasets.
- **Google Drive** — Cloud storage for raw and cleaned CSVs during intermediate ETL stages.

### 👨‍💻 Development & Testing Workflow

- **PyCharm + JupyterLab (Linux WSL2)** — Heavy-duty modular development in a full IDE with a Unix-native environment.  
- **PostgresSQL (Linux WSL2)** - SQL database for local testing to simulate Supabase.
- **Git + GitHub** — Full version control using `main`, `dev`, and modular feature branches.

### 🧠 Statistics & Machine Learning
 
- **statsmodels** - For linear/logistic regression, ANOVA, time series, and model diagnostics.
- **scikit-learn** — For ML pipelines: test/train split, transform/encode, classification, and clustering. 
- **PyTorch / TensorFlow** — For future deep learning use cases, especially time-series and tabular neural networks.

### 🔍 Visualization & Data Exploration (Streamlit-friendly)

- **plotly** - Interactive charting in Streamlit; great for user-defined dashboards
- **seaborn/matplotlib** - Automate custom plotting and visualization of outputs
- **altair** - Clean, declarative plotting; nice for statistical overlays and rule-based charts
- **ydata-profiling/pandas** - Embedded HTML reporting; great for EDA and validating data integrity
- **sweetviz** - Embedded HTML reporting; perfect for "before/after" pipeline analysis and comparison plots

---

## 🌿 Git Branch Structure

| Branch Name                  | Purpose                                                                     |
|------------------------------|-----------------------------------------------------------------------------|
| `main`                       | Production-ready, deployable branch                                         |
| `dev`                        | General development and integration staging branch                          |
| `etl/extract-validate-clean` | Scripts for extracting, validating, and cleaning data (Postgres coherent)   |
| `etl/duckdb-sql-filtering`   | Scripts for narrowing the use-case-specific scope of tables                 |
| `etl/transform-enrich`       | Scripts for transforming and enriching data (tabular and geospatial)        |
| `etl/load-to-supabase`       | Scripts for uploading structured tables to Supabase                         |
| `model/stats-ml`             | Scripts for statistics and ML pipelines based on Supabase data              |
| `vis/visualize-explore`      | Scripts for visualizing and exploring data and outputs from stats/ML        |     
| `docs/workflow-setup`        | Documentation about ETL workflows and setup instructions                    | 

---

## 📁 Repository Structure

```text
cloud-etl-postgres-ml/
├── data/
│   ├── raw/                    # Raw CSVs from Google Drive
│   └── cleaned/                # Cleaned datasets for Supabase
├── notebooks/
│   ├── eda_duckdb_colab.ipynb # DuckDB SQL queries and data exploration
│   └── model_dev.ipynb        # Optional: ML notebook
├── scripts/
│   ├── upload_to_supabase.py  # ETL logic: upload cleaned data
│   └── supabase_helpers.py    # DB connection and validation tools
├── sql/
│   └── schema.sql             # Optional: PostgreSQL schema
├── .env.example               # Template for Supabase DB credentials
├── .gitignore                 # Ignore sensitive and generated files
├── requirements.txt           # Python package dependencies
└── README.md                  # You are here.
```

---

## How to use this project

### 1: Clone and Set Up
bash

- git clone git@github.com:weston-ai/cloud-etl-postgres-ml
- cd cloud-etl-postgres-ml
- cp .env.example .env   # copies a template .env.example file to .env (avoids committing secrets)
    - E.G. *supabase\_db\_url = postgresql://postgres:[password]@db.hhzuypfvmrisuumznjid.supabase.co:5432/postgres (This is the url for postgres\_ML database in Supabase)*
- pip install -r requirements.txt  # installs Python dependencies listed in requirements.txt

### 2: Explore Raw Data in DuckDB (Colab)
- Open notebooks/eda\_duckdb\_colab.ipynb
- Mount Google Drive
- Run SQL queries on raw CSVs using DuckDB directly

### 3: Clean and Upload to Supabase
bash

python scripts/upload\_to\_supabase.py
- connects to supabase via .env
- Loads cleaned CSVs into Supabase PostgreSQL tables

## Notes
- Emphasizes clarity, modularity, and portability
- Can be designed to work entirely from a cloud-based stack (Colab, Supabase, Github, Google Drive)
- Supports mobile data workflows -- ideal for working from cabins, cafes, or campsites
- In reality, PyCharm via WSL2 will be helpful for doing the heavy dev-op lifting (i.e. modular function testing and scalability)

## Contact
Made by Chris Weston
- *Data Engineer & Data Scientist* | *Postgres ETL Pipelines* | *Lightweight Analytics & ML*
- Github Repo -- https://github.com/weston-ai/cloud-etl-postgres-ml
