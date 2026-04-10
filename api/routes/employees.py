"""
api/routes/employees.py
-----------------------
GET /employees — paginated employee listing with optional filters.
GET /employees/{employee_id} — single employee detail.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from api.database import get_db
from api.models import EmployeeResponse, PaginatedResponse
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/employees", tags=["Employees"])


@router.get("", response_model=PaginatedResponse, summary="List all employees")
def get_employees(
    page:         int = Query(1, ge=1, description="Page number"),
    page_size:    int = Query(50, ge=1, le=500, description="Records per page"),
    department:   str = Query(None, description="Filter by department"),
    status:       str = Query(None, description="Filter by status: Active | Inactive | On Leave"),
    db: Session = Depends(get_db)
):
    """
    Returns a paginated list of employees.
    Supports optional filtering by department and status.
    """
    conditions = []
    params = {}

    if department:
        conditions.append("department ILIKE :department")
        params["department"] = f"%{department}%"
    if status:
        conditions.append("status = :status")
        params["status"] = status

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Total count
    count_result = db.execute(
        text(f"SELECT COUNT(*) FROM employees {where_clause}"), params
    )
    total = count_result.scalar()

    # Paginated data
    params["limit"] = page_size
    params["offset"] = (page - 1) * page_size
    result = db.execute(
        text(f"""
            SELECT employee_id, first_name, last_name, department,
                   pay_rate, hire_date, status, email, created_at
            FROM employees {where_clause}
            ORDER BY last_name, first_name
            LIMIT :limit OFFSET :offset
        """), params
    )
    rows = [dict(r._mapping) for r in result]

    return PaginatedResponse(total=total, page=page, page_size=page_size, data=rows)


@router.get("/{employee_id}", summary="Get single employee by ID")
def get_employee(employee_id: str, db: Session = Depends(get_db)):
    """Returns complete profile for a single employee including payroll stats."""
    result = db.execute(
        text("""
            SELECT
                e.employee_id, e.first_name, e.last_name, e.department,
                e.pay_rate, e.hire_date, e.status, e.email, e.created_at,
                COUNT(t.log_id)                AS total_shifts,
                ROUND(SUM(t.hours_worked), 2)  AS total_hours,
                ROUND(SUM(t.overtime_hours), 2) AS total_overtime,
                ROUND(SUM(t.gross_pay), 2)      AS total_gross_pay
            FROM employees e
            LEFT JOIN time_logs t ON e.employee_id = t.employee_id
            WHERE e.employee_id = :employee_id
            GROUP BY e.employee_id, e.first_name, e.last_name, e.department,
                     e.pay_rate, e.hire_date, e.status, e.email, e.created_at
        """), {"employee_id": employee_id}
    )
    row = result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Employee {employee_id} not found")
    return dict(row._mapping)
