# вверху файла рядом с остальными импортами
from sqlalchemy import text

# ...
# (ничего лишнего, decimal не нужен — будем приводить к float в питоне)


from fastapi import FastAPI, HTTPException
from sqlalchemy import text
from .db import engine  # db.py подхватывает .env и создаёт engine


app = FastAPI(title="Payroll KPI API")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/kpi/summary")
def kpi_summary(month: str):
    sql = text(
        """
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
    """
    )
    with engine.begin() as con:
        row = con.execute(sql, {"m": month}).mappings().one()
    if row["gross"] is None:
        raise HTTPException(status_code=404, detail="No data for month")
    tax_share = (row["taxes"] / row["gross"]) if row["gross"] else None
    avg_net_per_fte = (row["net"] / row["fte"]) if row["fte"] else None
    return {**row, "tax_share": tax_share, "avg_net_per_fte": avg_net_per_fte}


@app.get("/kpi/by-dept")
def kpi_by_dept(month: str):
    sql = text(
        """
    WITH m AS (SELECT (:m || '-01')::date AS m)
    SELECT d.dept_name AS dept,
           SUM(f.gross)    AS gross,
           SUM(f.bonus)    AS bonus,
           SUM(f.overtime) AS overtime,
           SUM(f.gross+f.bonus+f.overtime) AS fot,
           SUM(f.net)      AS net,
           SUM(f.taxes)    AS taxes,
           SUM(f.fte)      AS fte,
           COUNT(DISTINCT f.emp_id) AS headcount
    FROM pr.fact_payroll f
    JOIN pr.dim_employee e ON e.emp_id = f.emp_id
    JOIN pr.dim_dept     d ON d.dept_id = e.dept_id
    JOIN m ON f.month = m.m
    GROUP BY d.dept_name
    ORDER BY d.dept_name;
    """
    )
    with engine.begin() as con:
        rows = con.execute(sql, {"m": month}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail="No data for month")
    return rows


@app.get("/kpi/delta")
def kpi_delta(m1: str, m2: str):
    sql_by_dept = text(
        """
    WITH m1 AS (SELECT (:m1 || '-01')::date AS m),
         m2 AS (SELECT (:m2 || '-01')::date AS m),
         a AS (
           SELECT d.dept_name AS dept,
                  SUM(f.gross) AS gross, SUM(f.bonus) AS bonus, SUM(f.overtime) AS overtime
           FROM pr.fact_payroll f
           JOIN pr.dim_employee e ON e.emp_id=f.emp_id
           JOIN pr.dim_dept d ON d.dept_id=e.dept_id
           JOIN m1 ON f.month=m1.m
           GROUP BY d.dept_name
         ),
         b AS (
           SELECT d.dept_name AS dept,
                  SUM(f.gross) AS gross, SUM(f.bonus) AS bonus, SUM(f.overtime) AS overtime
           FROM pr.fact_payroll f
           JOIN pr.dim_employee e ON e.emp_id=f.emp_id
           JOIN pr.dim_dept d ON d.dept_id=e.dept_id
           JOIN m2 ON f.month=m2.m
           GROUP BY d.dept_name
         )
    SELECT COALESCE(a.dept, b.dept) AS dept,
           COALESCE(b.gross,0)    - COALESCE(a.gross,0)    AS gross_delta,
           COALESCE(b.bonus,0)    - COALESCE(a.bonus,0)    AS bonus_delta,
           COALESCE(b.overtime,0) - COALESCE(a.overtime,0) AS overtime_delta,
           (COALESCE(b.gross,0)+COALESCE(b.bonus,0)+COALESCE(b.overtime,0))
         - (COALESCE(a.gross,0)+COALESCE(a.bonus,0)+COALESCE(a.overtime,0)) AS fot_delta
    FROM a FULL OUTER JOIN b ON a.dept = b.dept
    ORDER BY dept;
    """
    )
    sql_company = text(
        """
    WITH m1 AS (SELECT (:m1 || '-01')::date AS m),
         m2 AS (SELECT (:m2 || '-01')::date AS m)
    SELECT
      SUM(CASE WHEN month=(SELECT m FROM m1) THEN gross    ELSE 0 END) AS gross_m1,
      SUM(CASE WHEN month=(SELECT m FROM m1) THEN bonus    ELSE 0 END) AS bonus_m1,
      SUM(CASE WHEN month=(SELECT m FROM m1) THEN overtime ELSE 0 END) AS overtime_m1,
      SUM(CASE WHEN month=(SELECT m FROM m2) THEN gross    ELSE 0 END) AS gross_m2,
      SUM(CASE WHEN month=(SELECT m FROM m2) THEN bonus    ELSE 0 END) AS bonus_m2,
      SUM(CASE WHEN month=(SELECT m FROM m2) THEN overtime ELSE 0 END) AS overtime_m2
    FROM pr.fact_payroll;
    """
    )
    with engine.begin() as con:
        by_dept = con.execute(sql_by_dept, {"m1": m1, "m2": m2}).mappings().all()
        comp = con.execute(sql_company, {"m1": m1, "m2": m2}).mappings().one()

    gross_delta = (comp["gross_m2"] or 0) - (comp["gross_m1"] or 0)
    bonus_delta = (comp["bonus_m2"] or 0) - (comp["bonus_m1"] or 0)
    overtime_delta = (comp["overtime_m2"] or 0) - (comp["overtime_m1"] or 0)
    fot_delta = gross_delta + bonus_delta + overtime_delta

    return {
        "company": {
            "gross_delta": float(gross_delta or 0),
            "bonus_delta": float(bonus_delta or 0),
            "overtime_delta": float(overtime_delta or 0),
            "fot_delta": float(fot_delta or 0),
        },
        "by_dept": by_dept,
    }


@app.get("/kpi/anomalies")
def kpi_anomalies(
    month: str, threshold: float = 3.5, limit: int = 10, dept: str | None = None
):
    """
    Ищем сотрудников с нетипичным NET (робастный z-score по MAD).
    Параметры:
      month=YYYY-MM, threshold (обычно 3.5), limit, dept=...
    """
    base = """
    WITH m AS (SELECT (:m || '-01')::date AS m),
         data AS (
           SELECT e.emp_id, d.dept_name AS dept, f.net
           FROM pr.fact_payroll f
           JOIN pr.dim_employee e ON e.emp_id=f.emp_id
           JOIN pr.dim_dept d ON d.dept_id=e.dept_id
           JOIN m ON f.month = m.m
           {dept_filter}
         ),
         med AS (
           SELECT dept, percentile_cont(0.5) WITHIN GROUP (ORDER BY net) AS med
           FROM data GROUP BY dept
         ),
         dev AS (
           SELECT data.dept, abs(data.net - med.med) AS dev
           FROM data JOIN med USING(dept)
         ),
         mad AS (
           SELECT dept, percentile_cont(0.5) WITHIN GROUP (ORDER BY dev) AS mad
           FROM dev GROUP BY dept
         )
    SELECT data.emp_id, data.dept, data.net, med.med, mad.mad,
           CASE WHEN mad.mad = 0 THEN NULL
                ELSE 0.6745 * (data.net - med.med) / mad.mad END AS z
    FROM data
    JOIN med USING(dept)
    JOIN mad USING(dept)
    {dept_filter}
    HAVING mad.mad IS NOT NULL
    ORDER BY ABS(z) DESC NULLS LAST
    LIMIT :lim;
    """

    dept_where = ""
    params = {"m": month, "lim": limit}
    if dept:
        dept_where = "WHERE d.dept_name = :dept"  # для CTE data
        params["dept"] = dept
    sql = text(base.format(dept_filter=dept_where))

    with engine.begin() as con:
        rows = con.execute(sql, params).mappings().all()

    # фильтруем по порогу на приложенческом уровне (простее и прозрачно)
    rows = [r for r in rows if r["z"] is not None and abs(r["z"]) >= threshold]
    return rows


@app.get("/kpi/anomalies")
def kpi_anomalies(
    month: str, threshold: float = 3.5, limit: int = 10, dept: str | None = None
):
    """
    Топ-аномалии по NET за месяц на основе робастного z-score (MAD).
    Параметры:
      month=YYYY-MM, threshold=3.5 (|z| порог), limit=10, dept=... (опционально)
    """
    sql = """
    WITH params AS (SELECT (:m || '-01')::date AS m),
         data AS (
           SELECT e.emp_id, d.dept_name AS dept, f.net::numeric AS net
           FROM pr.fact_payroll f
           JOIN pr.dim_employee e ON e.emp_id = f.emp_id
           JOIN pr.dim_dept     d ON d.dept_id = e.dept_id
           JOIN params p        ON f.month = p.m
           {dept_where}
         ),
         med AS (
           SELECT dept, percentile_cont(0.5) WITHIN GROUP (ORDER BY net) AS med
           FROM data GROUP BY dept
         ),
         dev AS (
           SELECT data.dept, abs(data.net - med.med) AS dev
           FROM data JOIN med USING(dept)
         ),
         mad AS (
           SELECT dept, percentile_cont(0.5) WITHIN GROUP (ORDER BY dev) AS mad
           FROM dev GROUP BY dept
         )
    SELECT data.emp_id, data.dept, data.net, med.med, mad.mad,
           CASE
             WHEN COALESCE(mad.mad,0) = 0 THEN NULL
             ELSE 0.6745 * (data.net - med.med) / NULLIF(mad.mad,0)
           END AS z
    FROM data
    JOIN med USING(dept)
    JOIN mad USING(dept)
    ORDER BY ABS(
      COALESCE(
        CASE WHEN COALESCE(mad.mad,0)=0 THEN NULL
             ELSE 0.6745 * (data.net - med.med) / NULLIF(mad.mad,0)
        END, 0)
    ) DESC NULLS LAST
    LIMIT :lim;
    """
    where = "WHERE d.dept_name = :dept" if dept else ""
    params = {"m": month, "lim": limit}
    if dept:
        params["dept"] = dept

    with engine.begin() as con:
        rows = con.execute(text(sql.format(dept_where=where)), params).mappings().all()

    def f(x):  # Decimal → float (или None)
        return float(x) if x is not None else None

    # пост-фильтр по порогу и аккуратное приведение типов
    out = []
    for r in rows:
        z = r.get("z")
        if z is not None and abs(float(z)) < threshold:
            continue
        out.append(
            {
                "emp_id": r["emp_id"],
                "dept": r["dept"],
                "net": f(r["net"]),
                "median_net": f(r["med"]),
                "mad": f(r["mad"]),
                "z": f(r["z"]) if r["z"] is not None else None,
            }
        )
    return out
