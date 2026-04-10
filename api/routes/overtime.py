"""
api/routes/overtime.py
----------------------
GET /overtime-report           — overtime summary by department
GET /overtime-report/employees — top overtime earners
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from api.database import get_db
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/overtime-report", tags=["Overtime"])


@router.get("", summary="Overtime summary by department")
def get_overtime_report(db: Session = Depends(get_db)):
    """
    Returns overtime hours and cost broken down by department.
    Includes: total shifts, overtime shift count, overtime rate %.
    """
    result = db.execute(text("""
        SELECT
            department,
            COUNT(log_id)                                       AS total_shifts,
            SUM(CASE WHEN overtime_hours > 0 THEN 1 ELSE 0 END) AS overtime_shifts,
            ROUND(SUM(overtime_hours), 2)                       AS total_overtime_hours,
            ROUND(AVG(overtime_hours), 4)                       AS avg_overtime_per_shift,
            ROUND(SUM(overtime_pay), 2)                         AS total_overtime_cost,
            ROUND(
                SUM(CASE WHEN overtime_hours > 0 THEN 1 ELSE 0 END)::NUMERIC
                / NULLIF(COUNT(log_id), 0) * 100, 2
            )                                                   AS overtime_rate_pct,
            ROUND(
                SUM(overtime_pay) / NULLIF(SUM(gross_pay), 0) * 100, 2
            )                                                   AS overtime_cost_pct
        FROM time_logs
        GROUP BY department
        ORDER BY total_overtime_hours DESC
    """))
    return [dict(r._mapping) for r in result]


@router.get("/employees", summary="Top employees by overtime hours")
def get_top_overtime_employees(
    limit: int = Query(25, ge=1, le=200, description="Number of employees to return"),
    department: str = Query(None, description="Filter by department"),
    db: Session = Depends(get_db)
):
    """
    Returns employees ranked by overtime hours using window functions.
    Uses RANK() OVER (ORDER BY total_overtime DESC).
    """
    params: dict = {"limit": limit}
    dept_filter = ""
    if department:
        dept_filter = "AND t.department ILIKE :department"
        params["department"] = f"%{department}%"

    result = db.execute(text(f"""
        SELECT
            e.employee_id,
            e.first_name || ' ' || e.last_name             AS full_name,
            e.department,
            e.pay_rate,
            ROUND(SUM(t.hours_worked), 2)                  AS total_hours,
            ROUND(SUM(t.overtime_hours), 2)                AS total_overtime_hours,
            ROUND(SUM(t.overtime_pay), 2)                  AS total_overtime_cost,
            RANK() OVER (ORDER BY SUM(t.overtime_hours) DESC) AS overtime_rank
        FROM employees e
        JOIN time_logs t ON e.employee_id = t.employee_id
        WHERE 1=1 {dept_filter}
        GROUP BY e.employee_id, e.first_name, e.last_name, e.department, e.pay_rate
        ORDER BY total_overtime_hours DESC
        LIMIT :limit
    """), params)
    return [dict(r._mapping) for r in result]
