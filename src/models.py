from sqlalchemy import text
from .db import engine

DDL = """
CREATE SCHEMA IF NOT EXISTS pr;

CREATE TABLE IF NOT EXISTS pr.dim_dept(
  dept_id SERIAL PRIMARY KEY,
  dept_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS pr.dim_employee(
  emp_id TEXT PRIMARY KEY,
  dept_id INT NOT NULL REFERENCES pr.dim_dept(dept_id),
  job_grade TEXT,
  location  TEXT
);

CREATE TABLE IF NOT EXISTS pr.fact_payroll(
  emp_id TEXT NOT NULL REFERENCES pr.dim_employee(emp_id),
  month  DATE NOT NULL,
  gross  NUMERIC,
  bonus  NUMERIC,
  overtime NUMERIC,
  taxes  NUMERIC,
  deductions NUMERIC,
  net    NUMERIC,
  fte    NUMERIC,
  hours_worked NUMERIC,
  currency TEXT,
  PRIMARY KEY(emp_id, month)
);
"""

def ensure_schema():
    with engine.begin() as con:
        con.execute(text(DDL))
