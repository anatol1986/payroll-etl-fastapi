from sqlalchemy import text
from src.db import engine

with engine.begin() as con:
    rows = con.execute(text(
        "select indexname,indexdef "
        "from pg_indexes "
        "where schemaname='pr' and tablename='fact_payroll' "
        "order by 1"
    )).fetchall()

for name, ddl in rows:
    print(name, '->', ddl)
