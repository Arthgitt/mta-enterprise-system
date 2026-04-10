"""
api/models.py
-------------
Pydantic models for FastAPI request/response validation.
These define the shape of all API responses.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import date, datetime


# ──────────────────────────────────────────────
# EMPLOYEE MODELS
# ──────────────────────────────────────────────
class EmployeeBase(BaseModel):
    employee_id: str
    first_name: str
    last_name: str
    department: str
    pay_rate: float
    hire_date: date
    status: str
    email: Optional[str] = None

class EmployeeResponse(EmployeeBase):
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ──────────────────────────────────────────────
# TIME LOG MODELS
# ──────────────────────────────────────────────
class TimeLogResponse(BaseModel):
    log_id: str
    employee_id: str
    clock_in: datetime
    clock_out: datetime
    department: str
    hours_worked: float
    overtime_hours: float
    gross_pay: float
    log_date: date

    class Config:
        from_attributes = True


# ──────────────────────────────────────────────
# PAYROLL MODELS
# ──────────────────────────────────────────────
class PayrollSummaryResponse(BaseModel):
    employee_id: str
    week_start: date
    department: str
    total_hours: float
    total_overtime: float
    total_gross_pay: float
    num_shifts: int
    pay_period: str

    class Config:
        from_attributes = True


class DepartmentPayrollResponse(BaseModel):
    department: str
    total_employees: int
    total_hours: float
    total_gross_pay: float
    total_overtime_hours: float
    avg_hourly_rate: float


# ──────────────────────────────────────────────
# OVERTIME MODELS
# ──────────────────────────────────────────────
class OvertimeReportRow(BaseModel):
    department: str
    total_employees: int
    total_shifts: int
    total_overtime_hours: float
    avg_overtime_per_shift: float
    total_overtime_cost: float


class EmployeeOvertimeRow(BaseModel):
    employee_id: str
    full_name: str
    department: str
    total_hours: float
    overtime_hours: float
    overtime_cost: float


# ──────────────────────────────────────────────
# VALIDATION ERROR MODELS
# ──────────────────────────────────────────────
class ValidationErrorResponse(BaseModel):
    log_id: Optional[str]
    employee_id: Optional[str]
    clock_in: Optional[str]
    clock_out: Optional[str]
    department: Optional[str]
    error_reason: str
    flagged_at: Optional[datetime]

    class Config:
        from_attributes = True


# ──────────────────────────────────────────────
# GENERIC API RESPONSE WRAPPERS
# ──────────────────────────────────────────────
class PaginatedResponse(BaseModel):
    total: int
    page: int
    page_size: int
    data: list


class HealthResponse(BaseModel):
    status: str
    database: str
    version: str
    environment: str
