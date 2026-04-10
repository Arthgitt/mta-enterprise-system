-- =============================================================
-- MTA Enterprise Workforce System — Analytics SQL Queries
-- PostgreSQL 15+  |  Demonstrates advanced SQL for interviews
-- =============================================================


-- ──────────────────────────────────────────────────────────────
-- Q1: TOTAL HOURS PER EMPLOYEE (with name join)
-- Shows: JOIN, aggregation, ORDER BY
-- ──────────────────────────────────────────────────────────────
SELECT
    e.employee_id,
    e.first_name || ' ' || e.last_name   AS full_name,
    e.department,
    COUNT(t.log_id)                       AS total_shifts,
    ROUND(SUM(t.hours_worked), 2)         AS total_hours,
    ROUND(SUM(t.overtime_hours), 2)       AS total_overtime_hours,
    ROUND(SUM(t.gross_pay), 2)            AS total_gross_pay
FROM employees e
JOIN time_logs t ON e.employee_id = t.employee_id
WHERE e.status = 'Active'
GROUP BY e.employee_id, e.first_name, e.last_name, e.department
ORDER BY total_hours DESC;


-- ──────────────────────────────────────────────────────────────
-- Q2: OVERTIME BY DEPARTMENT (with HAVING filter)
-- Shows: GROUP BY, HAVING, aggregate filtering
-- ──────────────────────────────────────────────────────────────
SELECT
    department,
    COUNT(DISTINCT employee_id)           AS total_employees,
    COUNT(log_id)                         AS total_shifts,
    ROUND(SUM(overtime_hours), 2)         AS total_overtime_hours,
    ROUND(AVG(overtime_hours), 4)         AS avg_overtime_per_shift,
    ROUND(SUM(overtime_pay), 2)           AS total_overtime_cost
FROM time_logs
WHERE overtime_hours > 0
GROUP BY department
HAVING SUM(overtime_hours) > 50
ORDER BY total_overtime_hours DESC;


-- ──────────────────────────────────────────────────────────────
-- Q3: PAYROLL COST ANALYSIS BY DEPARTMENT AND MONTH
-- Shows: DATE_TRUNC, multi-level aggregation
-- ──────────────────────────────────────────────────────────────
SELECT
    department,
    DATE_TRUNC('month', log_date)::DATE   AS pay_month,
    COUNT(DISTINCT employee_id)           AS active_employees,
    ROUND(SUM(hours_worked), 2)           AS total_hours,
    ROUND(SUM(gross_pay), 2)              AS total_payroll_cost,
    ROUND(AVG(gross_pay), 2)              AS avg_shift_cost,
    ROUND(SUM(gross_pay) / NULLIF(SUM(hours_worked), 0), 2) AS effective_hourly_rate
FROM time_logs
GROUP BY department, DATE_TRUNC('month', log_date)
ORDER BY pay_month DESC, total_payroll_cost DESC;


-- ──────────────────────────────────────────────────────────────
-- Q4: RANK EMPLOYEES BY HOURS WORKED (Window Function)
-- Shows: RANK(), DENSE_RANK(), NTILE(), OVER(PARTITION BY)
-- ──────────────────────────────────────────────────────────────
SELECT
    e.employee_id,
    e.first_name || ' ' || e.last_name         AS full_name,
    e.department,
    ROUND(SUM(t.hours_worked), 2)               AS total_hours,
    RANK()       OVER (ORDER BY SUM(t.hours_worked) DESC)                      AS company_rank,
    DENSE_RANK() OVER (PARTITION BY e.department ORDER BY SUM(t.hours_worked) DESC) AS dept_rank,
    NTILE(4)     OVER (ORDER BY SUM(t.hours_worked) DESC)                      AS quartile
FROM employees e
JOIN time_logs t ON e.employee_id = t.employee_id
GROUP BY e.employee_id, e.first_name, e.last_name, e.department
ORDER BY company_rank;


-- ──────────────────────────────────────────────────────────────
-- Q5: RUNNING TOTAL OF PAYROLL COST (Window Function)
-- Shows: SUM() OVER (ORDER BY ...) — cumulative sum
-- ──────────────────────────────────────────────────────────────
SELECT
    DATE_TRUNC('week', log_date)::DATE          AS week_start,
    ROUND(SUM(gross_pay), 2)                    AS weekly_payroll,
    ROUND(
        SUM(SUM(gross_pay)) OVER (ORDER BY DATE_TRUNC('week', log_date)),
        2
    )                                           AS cumulative_payroll,
    COUNT(DISTINCT employee_id)                 AS employees_paid
FROM time_logs
GROUP BY DATE_TRUNC('week', log_date)
ORDER BY week_start;


-- ──────────────────────────────────────────────────────────────
-- Q6: OVERTIME PERCENTAGE BY DEPARTMENT
-- Shows: CASE WHEN, percentage calculations
-- ──────────────────────────────────────────────────────────────
SELECT
    department,
    COUNT(log_id)                               AS total_shifts,
    SUM(CASE WHEN overtime_hours > 0 THEN 1 ELSE 0 END) AS overtime_shifts,
    ROUND(
        SUM(CASE WHEN overtime_hours > 0 THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(log_id), 0) * 100, 2
    )                                           AS overtime_rate_pct,
    ROUND(SUM(overtime_pay) / NULLIF(SUM(gross_pay), 0) * 100, 2)
                                                AS overtime_cost_pct
FROM time_logs
GROUP BY department
ORDER BY overtime_rate_pct DESC;


-- ──────────────────────────────────────────────────────────────
-- Q7: TOP 10 MOST EXPENSIVE EMPLOYEES (CTE)
-- Shows: WITH ... AS (CTE), subquery patterns
-- ──────────────────────────────────────────────────────────────
WITH employee_costs AS (
    SELECT
        e.employee_id,
        e.first_name || ' ' || e.last_name          AS full_name,
        e.department,
        e.pay_rate,
        ROUND(SUM(t.hours_worked), 2)                AS total_hours,
        ROUND(SUM(t.gross_pay), 2)                   AS total_cost,
        ROUND(SUM(t.overtime_pay), 2)                AS total_overtime_cost
    FROM employees e
    JOIN time_logs t ON e.employee_id = t.employee_id
    GROUP BY e.employee_id, e.first_name, e.last_name, e.department, e.pay_rate
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY total_cost DESC) AS cost_rank
    FROM employee_costs
)
SELECT *
FROM ranked
WHERE cost_rank <= 10
ORDER BY cost_rank;


-- ──────────────────────────────────────────────────────────────
-- Q8: EMPLOYEE ATTENDANCE STREAK (LAG Window Function)
-- Shows: LAG(), date arithmetic, gap detection
-- ──────────────────────────────────────────────────────────────
WITH daily_attendance AS (
    SELECT
        employee_id,
        log_date,
        LAG(log_date) OVER (PARTITION BY employee_id ORDER BY log_date) AS prev_date,
        log_date - LAG(log_date) OVER (PARTITION BY employee_id ORDER BY log_date) AS gap_days
    FROM (
        SELECT DISTINCT employee_id, log_date FROM time_logs
    ) deduped
)
SELECT
    employee_id,
    log_date,
    prev_date,
    gap_days,
    CASE
        WHEN gap_days IS NULL OR gap_days > 1 THEN 'NEW_STREAK'
        ELSE 'CONTINUING'
    END AS streak_status
FROM daily_attendance
ORDER BY employee_id, log_date;
