import pandas as pd
from sqlalchemy import text
from src.db import engine
from src.models import ensure_schema

print('STEP 1: ensure_schema()')
ensure_schema()

print('STEP 2: read CSV')
df = pd.read_csv('data/payroll.csv')
print('csv rows=', len(df), 'cols=', list(df.columns))

# нормализация
df['dept'] = df['dept'].astype(str).str.strip()
if 'month' in df.columns:
    df['month'] = pd.to_datetime(df['month'].astype(str).str[:7] + '-01', errors='coerce')
for c in ['gross','bonus','overtime','taxes','deductions','net','fte','hours_worked']:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
if 'currency' not in df.columns:
    df['currency'] = 'BYN'

print('STEP 3: stage tables')
with engine.begin() as con:
    # depts
    df_dept = pd.DataFrame({'dept_name': sorted(df['dept'].dropna().unique())})
    df_dept.to_sql('x_dept', con, if_exists='replace', index=False)
    print('x_dept rows:', len(df_dept))

    # employees
    cols_emp = [c for c in ['emp_id','dept','job_grade','location'] if c in df.columns]
    if 'emp_id' not in df.columns:
        raise RuntimeError('Не найден столбец emp_id в CSV — без него загрузка невозможна')
    df_emp = df[cols_emp].drop_duplicates('emp_id')
    df_emp.to_sql('x_emp', con, if_exists='replace', index=False)
    print('x_emp rows:', len(df_emp))

    # fact
    cols_fact = [c for c in ['emp_id','month','gross','bonus','overtime','taxes','deductions','net','fte','hours_worked','currency'] if c in df.columns]
    df_fact = df[cols_fact].copy()
    df_fact.to_sql('x_fact', con, if_exists='replace', index=False)
    print('x_fact rows:', len(df_fact))

    print('STEP 4: upserts')

    r1 = con.execute(text("""
        INSERT INTO pr.dim_dept(dept_name)
        SELECT DISTINCT dept_name FROM x_dept
        ON CONFLICT (dept_name) DO NOTHING
    """))
    print('dim_dept inserted:', r1.rowcount)

    r2 = con.execute(text("""
        INSERT INTO pr.dim_employee(emp_id, dept_id, job_grade, location)
        SELECT x.emp_id, d.dept_id, x.job_grade, x.location
        FROM x_emp x JOIN pr.dim_dept d ON d.dept_name = x.dept
        ON CONFLICT (emp_id) DO UPDATE
          SET dept_id=EXCLUDED.dept_id, job_grade=EXCLUDED.job_grade, location=EXCLUDED.location
    """))
    print('dim_employee upserted:', r2.rowcount)

    r3 = con.execute(text("""
        INSERT INTO pr.fact_payroll(emp_id, month, gross, bonus, overtime, taxes, deductions, net, fte, hours_worked, currency)
        SELECT emp_id, month::date, gross, bonus, overtime, taxes, deductions, net, fte, hours_worked, currency
        FROM x_fact
        ON CONFLICT (emp_id, month) DO UPDATE SET
          gross=EXCLUDED.gross, bonus=EXCLUDED.bonus, overtime=EXCLUDED.overtime,
          taxes=EXCLUDED.taxes, deductions=EXCLUDED.deductions, net=EXCLUDED.net,
          fte=EXCLUDED.fte, hours_worked=EXCLUDED.hours_worked, currency=EXCLUDED.currency
    """))
    print('fact_payroll upserted:', r3.rowcount)

    total = con.execute(text("select count(*) from pr.fact_payroll")).scalar()
    print('TOTAL fact rows:', total)

print('DONE')
