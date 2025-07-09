# 🌐 cloud-etl-postgres-ml

This project showcases a **modular, cloud-native ETL and analytics pipeline** using Google Drive, DuckDB, Supabase (PostgreSQL), and Streamlit — all orchestrated from Colab and GitHub.

## 📌 Project Goals

- Ingest raw CSV data from Google Drive  
- Explore and clean datasets using DuckDB SQL in Google Colab 
- Load cleaned data into a Supabase-hosted PostgreSQL database  
- Visualize insights via an interactive Streamlit dashboard  
- Maintain a portable, lightweight data science stack from anywhere
- Use PyCharm for heavy dev-op scripting to control script modularity and scalability

---

## 🧠 System Architecture & Tool Roles

This project emphasizes a **modular, SQL-centric, cloud-native analytics pipeline** — blending Python, SQL, and machine learning workflows with a lightweight, mobile-friendly infrastructure.

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

### 🧠 Machine Learning

- **scikit-learn** — For ML pipelines: classification, regression, clustering, and evaluation.  
- **PyTorch** — For future deep learning use cases, especially time-series and tabular neural networks.

### 👨‍💻 Development Workflow

- **VSCode** — Lightweight IDE for quick edits, cloud or local.  
- **PyCharm (Linux WSL2)** — Heavy-duty modular development in a full IDE with a Unix-native environment.  
- **Git + GitHub** — Full version control using `main`, `dev`, and modular feature branches.

---

## 🌿 Git Branch Structure

| Branch Name              | Purpose                                                    |
|--------------------------|-------------------------------------------------------------|
| `main`                   | ✅ Production-ready, deployable branch                      |
| `dev`                    | 🧪 General development and integration staging branch        |
| `duckdb-analytics`       | 🔍 Colab notebooks for DuckDB-based SQL exploratory analysis |
| `etl-drive-to-supabase`  | 🔄 Python scripts for data cleaning + Supabase uploading     |
| `streamlit-dashboard`    | 📊 Frontend dashboard for analytics and visual exploration   |
| `ml-modeling` *(optional)* | 🤖 ML pipelines based on Supabase data                   |

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
├── streamlit/
│   └── app.py                 # Interactive dashboard UI
├── .env.example               # Template for Supabase DB credentials
├── .gitignore                 # Ignore sensitive and generated files
├── requirements.txt           # Python package dependencies
└── README.md                  # You are here.
```

## How to use this project

### 1: Clone and Set Up
bash

- git clone git@github.com:weston-ai/cloud-etl-postgres-ml
- cd cloud-etl-postgres-ml
- cp .env.example .env   # copies a template .env.example file to .env (avoids committing secrets)
    - E.G. *supabase\_db\_url = postgresql://postgres:[password]@db.hhzuypfvmrisuumznjid.supabase.co:5432/postgres (This is the url for postgres\_ML database in Supabase)*
- pip install -r requirements.txt  # installs Python dependencies listed in requirements.txt

### 2: Explore Raw Data in DuckDB (Colab)
- Open notebooks/eda_duckdb_colab.ipynb
- Mount Google Drive
- Run SQL queries on raw CSVs using DuckDB directly

### 3: Clean and Upload to Supabase
bash
python scripts/upload_to_supabase.py
- connects to supabase via .env
- Loads cleaned CSVs into Supabase PostgreSQL tables

### 4: Launch Dashboard
bash
cd streamlit
streamlit run app.py

## Notes
- Emphasizes clarity, modularity, and portability
- Can be designed to work entirely from a cloud-based stack (Colab, Supabase, Github, Google Drive)
- Supports mobile data workflows -- ideal for working from cabins, cafes, or campsites
- In reality, PyCharm via WSL2 will be helpful for doing the heavy dev-op lifting (i.e. modular function testing and scalability)

## Contact
Made by Chris Weston
- *Data Science* | *PostgreSQL* | *Lightweight ML*
- Github Repo -- https://github.com/weston-ai/cloud-etl-postgres-ml







