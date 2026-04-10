-- =============================================================
-- MTA Enterprise Workforce System — Database Schema
-- PostgreSQL 15+
-- =============================================================
-- Tables:
--   1. employees        — master employee directory
--   2. time_logs        — individual shift records (processed)
--   3. payroll_summary  — weekly aggregated payroll per employee
--   4. validation_errors — ETL data quality flags
-- =============================================================

-- ──────────────────────────────────────────────────────────────
-- 1. EMPLOYEES
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS employees (
    employee_id     VARCHAR(10)     PRIMARY KEY,
    first_name      VARCHAR(100)    NOT NULL,
    last_name       VARCHAR(100)    NOT NULL,
    department      VARCHAR(100)    NOT NULL DEFAULT 'UNKNOWN',
    pay_rate        NUMERIC(8, 2)   NOT NULL CHECK (pay_rate > 0),
    hire_date       DATE            NOT NULL,
    status          VARCHAR(20)     NOT NULL DEFAULT 'Active'
                                    CHECK (status IN ('Active', 'Inactive', 'On Leave')),
    email           VARCHAR(200)    UNIQUE,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_employees_department ON employees(department);
CREATE INDEX IF NOT EXISTS idx_employees_status ON employees(status);

-- Auto-update updated_at on row change
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_employees_updated_at
    BEFORE UPDATE ON employees
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();


-- ──────────────────────────────────────────────────────────────
-- 2. TIME_LOGS
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS time_logs (
    log_id          VARCHAR(12)     PRIMARY KEY,
    employee_id     VARCHAR(10)     NOT NULL REFERENCES employees(employee_id) ON DELETE CASCADE,
    clock_in        TIMESTAMP       NOT NULL,
    clock_out       TIMESTAMP       NOT NULL,
    department      VARCHAR(100)    NOT NULL,
    pay_rate        NUMERIC(8, 2)   NOT NULL,
    log_date        DATE            NOT NULL,

    -- Computed columns (populated by ETL)
    hours_worked    NUMERIC(6, 4)   NOT NULL CHECK (hours_worked > 0),
    overtime_hours  NUMERIC(6, 4)   NOT NULL DEFAULT 0 CHECK (overtime_hours >= 0),
    regular_hours   NUMERIC(6, 4)   NOT NULL CHECK (regular_hours > 0),
    regular_pay     NUMERIC(10, 2)  NOT NULL,
    overtime_pay    NUMERIC(10, 2)  NOT NULL DEFAULT 0,
    gross_pay       NUMERIC(10, 2)  NOT NULL,

    CONSTRAINT chk_clock_sequence CHECK (clock_out > clock_in)
);

CREATE INDEX IF NOT EXISTS idx_time_logs_employee  ON time_logs(employee_id);
CREATE INDEX IF NOT EXISTS idx_time_logs_log_date  ON time_logs(log_date);
CREATE INDEX IF NOT EXISTS idx_time_logs_department ON time_logs(department);
CREATE INDEX IF NOT EXISTS idx_time_logs_clock_in  ON time_logs(clock_in);


-- ──────────────────────────────────────────────────────────────
-- 3. PAYROLL_SUMMARY
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS payroll_summary (
    id              SERIAL          PRIMARY KEY,
    employee_id     VARCHAR(10)     NOT NULL REFERENCES employees(employee_id) ON DELETE CASCADE,
    week_start      DATE            NOT NULL,
    department      VARCHAR(100)    NOT NULL,
    total_hours     NUMERIC(8, 2)   NOT NULL,
    total_overtime  NUMERIC(8, 2)   NOT NULL DEFAULT 0,
    total_gross_pay NUMERIC(12, 2)  NOT NULL,
    num_shifts      INTEGER         NOT NULL DEFAULT 0,
    pay_period      VARCHAR(20)     NOT NULL DEFAULT 'weekly',
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    UNIQUE (employee_id, week_start)
);

CREATE INDEX IF NOT EXISTS idx_payroll_employee  ON payroll_summary(employee_id);
CREATE INDEX IF NOT EXISTS idx_payroll_week      ON payroll_summary(week_start);
CREATE INDEX IF NOT EXISTS idx_payroll_dept      ON payroll_summary(department);


-- ──────────────────────────────────────────────────────────────
-- 4. VALIDATION_ERRORS
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS validation_errors (
    id              SERIAL          PRIMARY KEY,
    log_id          VARCHAR(12),
    employee_id     VARCHAR(10),
    clock_in        VARCHAR(30),
    clock_out       VARCHAR(30),
    department      VARCHAR(100),
    error_reason    VARCHAR(100)    NOT NULL,
    flagged_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id VARCHAR(30)
);

CREATE INDEX IF NOT EXISTS idx_valerr_error_reason ON validation_errors(error_reason);
CREATE INDEX IF NOT EXISTS idx_valerr_flagged_at   ON validation_errors(flagged_at);
