-- =============================================================
-- MTA Enterprise Workforce — BigQuery Analytics Queries
-- Project: mta_workforce_analytics
-- Dataset: mta_workforce_analytics
-- =============================================================


-- ──────────────────────────────────────────────────────────────
-- BQ1: DEPARTMENT PAYROLL COST TREND BY MONTH
-- Uses: DATE_TRUNC, partitioning, OVER clause
-- ──────────────────────────────────────────────────────────────
SELECT
    department,
    DATE_TRUNC(log_date, MONTH)                             AS pay_month,
    COUNT(DISTINCT employee_id)                             AS active_employees,
    ROUND(SUM(hours_worked), 2)                             AS total_hours,
    ROUND(SUM(gross_pay), 2)                                AS total_payroll,
    ROUND(
        SUM(SUM(gross_pay)) OVER (
            PARTITION BY department
            ORDER BY DATE_TRUNC(log_date, MONTH)
        ), 2
    )                                                       AS cumulative_dept_payroll
FROM `mta_workforce_analytics.time_logs`
GROUP BY department, DATE_TRUNC(log_date, MONTH)
ORDER BY department, pay_month;


-- ──────────────────────────────────────────────────────────────
-- BQ2: OVERTIME DISTRIBUTION BUCKETS (histogram)
-- ──────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN overtime_hours = 0             THEN '0 hrs'
        WHEN overtime_hours BETWEEN 0.01 AND 1  THEN '0-1 hrs'
        WHEN overtime_hours BETWEEN 1 AND 2     THEN '1-2 hrs'
        WHEN overtime_hours BETWEEN 2 AND 4     THEN '2-4 hrs'
        WHEN overtime_hours > 4             THEN '4+ hrs'
    END                                                     AS overtime_bucket,
    COUNT(*)                                                AS shift_count,
    ROUND(AVG(gross_pay), 2)                                AS avg_gross_pay,
    ROUND(SUM(overtime_pay), 2)                             AS total_overtime_cost
FROM `mta_workforce_analytics.time_logs`
GROUP BY overtime_bucket
ORDER BY shift_count DESC;


-- ──────────────────────────────────────────────────────────────
-- BQ3: TOP 10 DEPARTMENTS BY HEADCOUNT & COST
-- ──────────────────────────────────────────────────────────────
SELECT
    department,
    COUNT(DISTINCT employee_id)                             AS headcount,
    ROUND(SUM(gross_pay), 2)                                AS total_cost,
    ROUND(AVG(pay_rate), 2)                                 AS avg_pay_rate,
    ROUND(SUM(overtime_hours) / NULLIF(SUM(hours_worked), 0) * 100, 2) AS ot_rate_pct
FROM `mta_workforce_analytics.time_logs`
GROUP BY department
ORDER BY headcount DESC
LIMIT 10;


-- ──────────────────────────────────────────────────────────────
-- BQ4: WEEKLY PAYROLL FORECAST (linear trend using REGR)
-- ──────────────────────────────────────────────────────────────
WITH weekly AS (
    SELECT
        DATE_TRUNC(log_date, WEEK)                          AS week_start,
        SUM(gross_pay)                                      AS weekly_payroll
    FROM `mta_workforce_analytics.time_logs`
    GROUP BY DATE_TRUNC(log_date, WEEK)
),
numbered AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY week_start) AS week_num
    FROM weekly
)
SELECT
    week_start,
    ROUND(weekly_payroll, 2)                                AS actual_payroll,
    ROUND(
        AVG(weekly_payroll) OVER (
            ORDER BY week_num
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ), 2
    )                                                       AS moving_avg_4wk
FROM numbered
ORDER BY week_start;
