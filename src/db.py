import os
from pathlib import Path
from dotenv import load_dotenv, dotenv_values
from sqlalchemy import create_engine

# корень проекта = на уровень выше /src
ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"

# 1) пробуем загрузить в os.environ
load_dotenv(ENV_PATH)

# 2) fallback: читаем ключ напрямую из файла .env (на случай, если os.environ не заполнился)
DB_URL = os.getenv("DB_URL") or dotenv_values(ENV_PATH).get("DB_URL")

if not DB_URL:
    raise RuntimeError(f"DB_URL is not set. Expected in {ENV_PATH}")

engine = create_engine(DB_URL, future=True)
