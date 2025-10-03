from fastapi import FastAPI, HTTPException
from sqlalchemy import text
from .db import engine

app = FastAPI(title="Payroll KPI API")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/kpi/summary")
def kpi_summary(month: str):
    sql = text(\"""
    WITH m AS (SELECT (:m || '-01')::date AS m)
    SELECT 
      SUM(gross+bonus+overtime) AS fot,
      SUM(taxes) AS taxes,
      SUM(gross) AS gross,
      SUM(net) AS net,
      SUM(fte) AS fte,
      COUNT(DISTINCT emp_id) AS headcount
    FROM pr.fact_payroll, m
    WHERE month = m.m;
    \""")
    with engine.begin() as con:
        row = con.execute(sql, {"m": month}).mappings().one()
    if row["gross"] is None:
        raise HTTPException(status_code=404, detail="No data for month")
    tax_share = (row["taxes"]/row["gross"]) if row["gross"] else None
    avg_net_per_fte = (row["net"]/row["fte"]) if row["fte"] else None
    return {**row, "tax_share": tax_share, "avg_net_per_fte": avg_net_per_fte}
