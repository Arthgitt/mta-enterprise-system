// =============================================================
// ApiClient.cs — MTA Workforce Admin Dashboard
// HttpClient wrapper for all FastAPI endpoint calls.
// Demonstrates: API consumption, async/await, error handling
// =============================================================

using System.Net.Http.Json;
using System.Text.Json;

namespace WorkforceAdmin;

public class WorkforceApiClient : IDisposable
{
    private readonly HttpClient _http;
    private readonly string _baseUrl;
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public WorkforceApiClient(string baseUrl)
    {
        _baseUrl = baseUrl.TrimEnd('/');
        _http = new HttpClient
        {
            BaseAddress = new Uri(_baseUrl),
            Timeout = TimeSpan.FromSeconds(30)
        };
        _http.DefaultRequestHeaders.Add("Accept", "application/json");
        _http.DefaultRequestHeaders.Add("User-Agent", "WorkforceAdmin-CSharp/1.0");
    }

    // ── Employees ──────────────────────────────────────────────

    public async Task<PaginatedResponse<Employee>?> GetEmployeesAsync(
        int page = 1, int pageSize = 50, string? department = null, string? status = null)
    {
        var query = BuildQueryString(
            ("page", page.ToString()),
            ("page_size", pageSize.ToString()),
            ("department", department),
            ("status", status)
        );
        return await GetJsonAsync<PaginatedResponse<Employee>>($"/employees{query}");
    }

    public async Task<Employee?> GetEmployeeDetailAsync(string employeeId)
        => await GetJsonAsync<Employee>($"/employees/{employeeId}");

    // ── Payroll ────────────────────────────────────────────────

    public async Task<List<DepartmentPayroll>?> GetDepartmentPayrollAsync()
        => await GetJsonAsync<List<DepartmentPayroll>>("/payroll/department-summary");

    public async Task<List<dynamic>?> GetWeeklyTrendAsync(int weeks = 12)
        => await GetJsonAsync<List<dynamic>>($"/payroll/weekly-trend?weeks={weeks}");

    // ── Overtime ───────────────────────────────────────────────

    public async Task<List<OvertimeReport>?> GetOvertimeReportAsync()
        => await GetJsonAsync<List<OvertimeReport>>("/overtime-report");

    public async Task<List<Employee>?> GetTopOvertimeEmployeesAsync(int limit = 25)
        => await GetJsonAsync<List<Employee>>($"/overtime-report/employees?limit={limit}");

    // ── Validation ─────────────────────────────────────────────

    public async Task<ValidationErrorsResponse?> GetValidationErrorsAsync(
        string? errorType = null, int limit = 50)
    {
        var query = BuildQueryString(
            ("error_type", errorType),
            ("limit", limit.ToString())
        );
        return await GetJsonAsync<ValidationErrorsResponse>($"/validation-errors{query}");
    }

    // ── Health ─────────────────────────────────────────────────

    public async Task<bool> CheckHealthAsync()
    {
        try
        {
            var response = await _http.GetAsync("/health");
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    // ── Helpers ────────────────────────────────────────────────

    private async Task<T?> GetJsonAsync<T>(string path)
    {
        try
        {
            var response = await _http.GetAsync(path);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadFromJsonAsync<T>(JsonOptions);
        }
        catch (HttpRequestException ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"  ✗ API error [{path}]: {ex.Message}");
            Console.ResetColor();
            return default;
        }
        catch (TaskCanceledException)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"  ⚠ Request timed out: {path}");
            Console.ResetColor();
            return default;
        }
    }

    private static string BuildQueryString(params (string Key, string? Value)[] pairs)
    {
        var parts = pairs
            .Where(p => p.Value != null)
            .Select(p => $"{p.Key}={Uri.EscapeDataString(p.Value!)}");
        var qs = string.Join("&", parts);
        return qs.Length > 0 ? "?" + qs : "";
    }

    public void Dispose() => _http.Dispose();
}
