"""
pipeline/load.py
----------------
Load layer: persists cleaned, transformed data into PostgreSQL.

Uses SQLAlchemy Core (not ORM) for high-performance bulk inserts.
Implements upsert logic to make the ETL idempotent (safe to re-run).
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from pipeline.logger import get_logger
from dotenv import load_dotenv

load_dotenv()
logger = get_logger(__name__)


def get_engine():
    """Create and return a SQLAlchemy engine from DATABASE_URL env var."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise EnvironmentError("DATABASE_URL environment variable is not set.")
    engine = create_engine(db_url, pool_pre_ping=True, pool_size=5, max_overflow=10)
    return engine


def load_employees(df: pd.DataFrame) -> int:
    """
    Upserts employee records into the `employees` table.
    Returns number of rows inserted/updated.
    """
    engine = get_engine()
    logger.info(f"Loading {len(df):,} employees into PostgreSQL...")

    rows_affected = 0
    with engine.connect() as conn:
        for _, row in df.iterrows():
            result = conn.execute(
                text("""
                    INSERT INTO employees (
                        employee_id, first_name, last_name, department,
                        pay_rate, hire_date, status, email
                    ) VALUES (
                        :employee_id, :first_name, :last_name, :department,
                        :pay_rate, :hire_date, :status, :email
                    )
                    ON CONFLICT (employee_id) DO UPDATE SET
                        department  = EXCLUDED.department,
                        pay_rate    = EXCLUDED.pay_rate,
                        status      = EXCLUDED.status,
                        email       = EXCLUDED.email
                """),
                row.to_dict()
            )
            rows_affected += result.rowcount
        conn.commit()

    logger.info(f"  ✓ Employees upserted: {rows_affected:,} rows")
    return rows_affected


def load_time_logs(df: pd.DataFrame) -> int:
    """
    Bulk-inserts processed time logs. Skips rows already in DB (by log_id).
    Returns number of rows inserted.
    """
    engine = get_engine()
    logger.info(f"Loading {len(df):,} time logs into PostgreSQL...")

    columns = [
        "log_id", "employee_id", "clock_in", "clock_out",
        "department", "pay_rate", "log_date",
        "hours_worked", "overtime_hours", "regular_hours",
        "regular_pay", "overtime_pay", "gross_pay"
    ]
    df_subset = df[columns].copy()

    # Use pandas to_sql with 'append' + duplicate protection at DB level
    engine_raw = get_engine()
    with engine_raw.connect() as conn:
        # Temp staging table approach for bulk upsert
        df_subset.to_sql("_time_logs_staging", conn, if_exists="replace", index=False)
        conn.execute(text("""
            INSERT INTO time_logs (
                log_id, employee_id, clock_in, clock_out,
                department, pay_rate, log_date,
                hours_worked, overtime_hours, regular_hours,
                regular_pay, overtime_pay, gross_pay
            )
            SELECT
                log_id, employee_id, clock_in::timestamp, clock_out::timestamp,
                department, pay_rate, log_date::date,
                hours_worked, overtime_hours, regular_hours,
                regular_pay, overtime_pay, gross_pay
            FROM _time_logs_staging
            ON CONFLICT (log_id) DO NOTHING
        """))
        result = conn.execute(text("SELECT COUNT(*) FROM time_logs"))
        total = result.scalar()
        conn.commit()

    logger.info(f"  ✓ Time logs loaded. Total in DB: {total:,}")
    return len(df_subset)


def load_payroll_summary(df: pd.DataFrame) -> int:
    """
    Upserts weekly payroll summary records into payroll_summary table.
    """
    engine = get_engine()
    logger.info(f"Loading {len(df):,} payroll summary rows into PostgreSQL...")

    with engine.connect() as conn:
        df.to_sql("_payroll_staging", conn, if_exists="replace", index=False)
        conn.execute(text("""
            INSERT INTO payroll_summary (
                employee_id, week_start, department,
                total_hours, total_overtime, total_gross_pay,
                num_shifts, pay_period
            )
            SELECT
                employee_id, week_start::date, department,
                total_hours, total_overtime, total_gross_pay,
                num_shifts, pay_period
            FROM _payroll_staging
            ON CONFLICT (employee_id, week_start) DO UPDATE SET
                total_hours     = EXCLUDED.total_hours,
                total_overtime  = EXCLUDED.total_overtime,
                total_gross_pay = EXCLUDED.total_gross_pay,
                num_shifts      = EXCLUDED.num_shifts
        """))
        conn.commit()

    logger.info(f"  ✓ Payroll summary upserted: {len(df):,} rows")
    return len(df)
