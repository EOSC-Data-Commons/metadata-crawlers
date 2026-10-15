"""
Environment / configuration loading.

Reads USER, PASSWORD, and DATABASE from a local .env file (via
load_dotenv()) and builds the Postgres connection string used by db.py.
"""

import os
from dotenv import load_dotenv

load_dotenv()

user = os.environ["USER"]
password = os.environ["PASSWORD"]
database = os.environ["DATABASE"]

DATABASE_URL = f"postgresql://{user}:{password}@localhost:5432/{database}"