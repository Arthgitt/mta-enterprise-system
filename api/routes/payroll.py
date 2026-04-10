"""
api/routes/payroll.py
---------------------
GET /payroll                    — paginated payroll summary records
GET /payroll/department-summary — aggregate payroll cost by department
GET /payroll/weekly-trend       — weekly payroll trend (last N weeks)
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from api.database import get_db
from api.models import PaginatedResponse
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/payroll", tags=["Payroll"])


@router.get("", response_model=PaginatedResponse, summary="Get payroll summary records")
def get_payroll(
    page:       int = Query(1, ge=1),
    page_size:  int = Query(50, ge=1, le=500),
    department: str = Query(None),
    week_start: str = Query(None, description="Filter by week start date (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """Returns paginated payroll summary records with optional filters."""
    conditions = []
    params: dict = {}

    if department:
        conditions.append("department ILIKE :department")
        params["department"] = f"%{department}%"
    if week_start:
        conditions.append("week_start = :week_start")
        params["week_start"] = week_start

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    total = db.execute(
        text(f"SELECT COUNT(*) FROM payroll_summary {where_clause}"), params
    ).scalar()

    params["limit"] = page_size
    params["offset"] = (page - 1) * page_size
    result = db.execute(
        text(f"""
            SELECT ps.*, e.first_name || ' ' || e.last_name AS full_name
            FROM payroll_summary ps
            JOIN employees e ON ps.employee_id = e.employee_id
            {where_clause}
            ORDER BY ps.week_start DESC, ps.total_gross_pay DESC
            LIMIT :limit OFFSET :offset
        """), params
    )
    rows = [dict(r._mapping) for r in result]
    return PaginatedResponse(total=total, page=page, page_size=page_size, data=rows)


@router.get("/department-summary", summary="Payroll cost aggregated by department")
def get_department_payroll_summary(db: Session = Depends(get_db)):
    """
    Returns total payroll cost, hours, and headcount aggregated by department.
    Great for dashboard KPI cards.
    """
    result = db.execute(text("""
        SELECT
            t.department,
            COUNT(DISTINCT t.employee_id)           AS total_employees,
            ROUND(SUM(t.hours_worked), 2)           AS total_hours,
            ROUND(SUM(t.overtime_hours), 2)         AS total_overtime_hours,
            ROUND(SUM(t.gross_pay), 2)              AS total_gross_pay,
            ROUND(AVG(t.pay_rate), 2)               AS avg_pay_rate,
            ROUND(
                SUM(t.gross_pay) / NULLIF(SUM(t.hours_worked), 0), 2
            )                                       AS effective_hourly_rate
        FROM time_logs t
        GROUP BY t.department
        ORDER BY total_gross_pay DESC
    """))
    return [dict(r._mapping) for r in result]


@router.get("/weekly-trend", summary="Weekly payroll cost trend")
def get_weekly_payroll_trend(
    weeks: int = Query(12, ge=1, le=52, description="Number of past weeks to include"),
    db: Session = Depends(get_db)
):
    """
    Returns weekly payroll totals for trend analysis and charting.
    Used by the analytics dashboard line chart.
    """
    result = db.execute(text("""
        SELECT
            DATE_TRUNC('week', log_date)::DATE         AS week_start,
            ROUND(SUM(gross_pay), 2)                   AS weekly_payroll,
            ROUND(SUM(overtime_pay), 2)                AS weekly_overtime_cost,
            ROUND(SUM(hours_worked), 2)                AS weekly_hours,
            COUNT(DISTINCT employee_id)                AS employees_paid,
            ROUND(
                SUM(SUM(gross_pay)) OVER (ORDER BY DATE_TRUNC('week', log_date)),
                2
            )                                          AS cumulative_payroll
        FROM time_logs
        GROUP BY DATE_TRUNC('week', log_date)
        ORDER BY week_start DESC
        LIMIT :weeks
    """), {"weeks": weeks})
    rows = [dict(r._mapping) for r in result]
    return list(reversed(rows))  # Return chronological order
