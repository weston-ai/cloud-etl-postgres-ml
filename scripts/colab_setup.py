# colab_setup.py
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    raise ImportError("❌ python-dotenv is not installed. Run `!pip install python-dotenv` in Colab first.")

# Set BASE_DIR to current working directory
BASE_DIR = Path().resolve()

# Ensure BASE_DIR is in sys.path
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

# Load environment variables from .env if it exists
env_path = BASE_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path)
    print(f"✅ Loaded environment variables from {env_path}")
else:
    print("⚠️ No .env file found in project root.")

# Test import of local utility package
try:
    from weston_utils.postgres_utils import connect_to_db
    print("✅ Successfully imported weston_utils.postgres_utils")
except ImportError as e:
    print(f"❌ Failed to import weston_utils.postgres_utils: {e}")