import pandas as pd
from sqlalchemy import text
from .db import engine
from .models import ensure_schema

def load_csv(path="data/payroll.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df["dept"] = df["dept"].astype(str).str.strip()
    df["month"] = pd.to_datetime(df["month"] + "-01")
    for c in ["gross","bonus","overtime","taxes","deductions","net","fte","hours_worked"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df

def upsert_all(df: pd.DataFrame):
    with engine.begin() as con:
        # depts
        depts = pd.DataFrame({"dept_name": sorted(df["dept"].unique())})
        depts.to_sql("x_dept", con, if_exists="replace", index=False)
        con.execute(text(\"""
            INSERT INTO pr.dim_dept(dept_name)
            SELECT DISTINCT dept_name FROM x_dept
            ON CONFLICT (dept_name) DO NOTHING;
        \"""))

        # employees
        cols = ["emp_id","dept","job_grade","location"]
        emp = df[[c for c in cols if c in df.columns]].drop_duplicates("emp_id").copy()
        emp["dept"] = emp["dept"].astype(str).str.strip()
        emp.to_sql("x_emp", con, if_exists="replace", index=False)
        con.execute(text(\"""
            INSERT INTO pr.dim_employee(emp_id, dept_id, job_grade, location)
            SELECT x.emp_id, d.dept_id, x.job_grade, x.location
            FROM x_emp x JOIN pr.dim_dept d ON d.dept_name = x.dept
            ON CONFLICT (emp_id) DO UPDATE
              SET dept_id=EXCLUDED.dept_id, job_grade=EXCLUDED.job_grade, location=EXCLUDED.location;
        \"""))

        # fact
        fact = df[["emp_id","month","gross","bonus","overtime","taxes","deductions","net","fte","hours_worked","currency"]].copy()
        fact.to_sql("x_fact", con, if_exists="replace", index=False)
        con.execute(text(\"""
            INSERT INTO pr.fact_payroll(emp_id, month, gross, bonus, overtime, taxes, deductions, net, fte, hours_worked, currency)
            SELECT emp_id, month::date, gross, bonus, overtime, taxes, deductions, net, fte, hours_worked, currency
            FROM x_fact
            ON CONFLICT (emp_id, month) DO UPDATE SET
              gross=EXCLUDED.gross, bonus=EXCLUDED.bonus, overtime=EXCLUDED.overtime,
              taxes=EXCLUDED.taxes, deductions=EXCLUDED.deductions, net=EXCLUDED.net,
              fte=EXCLUDED.fte, hours_worked=EXCLUDED.hours_worked, currency=EXCLUDED.currency;
        \"""))

if __name__ == "__main__":
    ensure_schema()
    df = load_csv()
    upsert_all(df)
    print("ETL: OK")
