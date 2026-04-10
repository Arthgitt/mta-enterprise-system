// =============================================================
// Models.cs — MTA Workforce Admin Dashboard
// POCOs that mirror the FastAPI JSON response shapes.
// =============================================================

using System.Text.Json.Serialization;

namespace WorkforceAdmin;

/// <summary>Employee record returned by GET /employees</summary>
public record Employee(
    [property: JsonPropertyName("employee_id")]   string EmployeeId,
    [property: JsonPropertyName("first_name")]    string FirstName,
    [property: JsonPropertyName("last_name")]     string LastName,
    [property: JsonPropertyName("department")]    string Department,
    [property: JsonPropertyName("pay_rate")]      decimal PayRate,
    [property: JsonPropertyName("hire_date")]     string HireDate,
    [property: JsonPropertyName("status")]        string Status,
    [property: JsonPropertyName("email")]         string? Email,
    [property: JsonPropertyName("total_shifts")]  int? TotalShifts,
    [property: JsonPropertyName("total_hours")]   decimal? TotalHours,
    [property: JsonPropertyName("total_gross_pay")] decimal? TotalGrossPay
)
{
    public string FullName => $"{FirstName} {LastName}";
    public bool IsOvertime => TotalHours.HasValue && TotalHours > 40;
}

/// <summary>Paginated API response wrapper</summary>
public record PaginatedResponse<T>(
    [property: JsonPropertyName("total")]     int Total,
    [property: JsonPropertyName("page")]      int Page,
    [property: JsonPropertyName("page_size")] int PageSize,
    [property: JsonPropertyName("data")]      List<T> Data
);

/// <summary>Department payroll summary from GET /payroll/department-summary</summary>
public record DepartmentPayroll(
    [property: JsonPropertyName("department")]          string Department,
    [property: JsonPropertyName("total_employees")]     int TotalEmployees,
    [property: JsonPropertyName("total_hours")]         decimal TotalHours,
    [property: JsonPropertyName("total_gross_pay")]     decimal TotalGrossPay,
    [property: JsonPropertyName("total_overtime_hours")] decimal TotalOvertimeHours,
    [property: JsonPropertyName("avg_pay_rate")]        decimal AvgPayRate,
    [property: JsonPropertyName("effective_hourly_rate")] decimal EffectiveHourlyRate
)
{
    /// <summary>Computed: overtime as % of total payroll hours</summary>
    public decimal OvertimeRatePct =>
        TotalHours > 0 ? Math.Round(TotalOvertimeHours / TotalHours * 100, 1) : 0;
}

/// <summary>Overtime report row from GET /overtime-report</summary>
public record OvertimeReport(
    [property: JsonPropertyName("department")]          string Department,
    [property: JsonPropertyName("total_shifts")]        int TotalShifts,
    [property: JsonPropertyName("overtime_shifts")]     int OvertimeShifts,
    [property: JsonPropertyName("total_overtime_hours")] decimal TotalOvertimeHours,
    [property: JsonPropertyName("total_overtime_cost")] decimal TotalOvertimeCost,
    [property: JsonPropertyName("overtime_rate_pct")]   decimal OvertimeRatePct
);

/// <summary>Validation error from GET /validation-errors</summary>
public record ValidationError(
    [property: JsonPropertyName("log_id")]        string? LogId,
    [property: JsonPropertyName("employee_id")]   string? EmployeeId,
    [property: JsonPropertyName("clock_in")]      string? ClockIn,
    [property: JsonPropertyName("clock_out")]     string? ClockOut,
    [property: JsonPropertyName("error_reason")]  string ErrorReason,
    [property: JsonPropertyName("flagged_at")]    string? FlaggedAt
);

public record ValidationErrorsResponse(
    [property: JsonPropertyName("total")]    int Total,
    [property: JsonPropertyName("showing")]  int Showing,
    [property: JsonPropertyName("errors")]   List<ValidationError> Errors
);
