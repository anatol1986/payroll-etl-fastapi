import os
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine

# грузим .env независимо от текущей рабочей директории
load_dotenv(find_dotenv())

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL is not set. Put it into .env")
engine = create_engine(DB_URL, future=True)
