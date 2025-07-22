from setuptools import setup, find_packages

setup(
    name="weston_utils",
    version="0.1.0",
    description="Reusable utility modules for PostgreSQL, logging, and analytics.",
    author="Chris Weston",
    python_requires=">=3.8",
    long_description=open("README_weston_utils.md").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(where="."),
    install_requires=[
        "pandas>=1.3.0,<3.0.0",
        "sqlalchemy>=1.4.0",
        "psycopg2-binary>=2.8.6",
        "python-dotenv>=0.19.0",
    ],
    project_urls={
        "Homepage": "https://github.com/weston-ai/cloud-etl-postgres-ml",
        "Repository": "https://github.com/weston-ai/cloud-etl-postgres-ml",
        "Dev Branch": "https://github.com/weston-ai/cloud-etl-postgres-ml/tree/dev",
        "Source (weston_utils)": "https://github.com/weston-ai/cloud-etl-postgres-ml/tree/dev/weston_utils",
    },
)
