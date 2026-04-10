// =============================================================
// Program.cs — MTA Workforce Admin Console Dashboard
// Demonstrates: HttpClient, JSON deserialization, business logic
//
// Usage:
//   cd csharp/WorkforceAdmin
//   dotnet run
//   dotnet run -- --api-url http://localhost:8000
// =============================================================

using WorkforceAdmin;

// ── Config ───────────────────────────────────────────────────
var apiUrl = args.Length > 1 && args[0] == "--api-url"
    ? args[1]
    : Environment.GetEnvironmentVariable("API_BASE_URL") ?? "http://localhost:8000";

using var client = new WorkforceApiClient(apiUrl);

// ── Banner ───────────────────────────────────────────────────
PrintBanner(apiUrl);

// ── Health Check ─────────────────────────────────────────────
Console.Write("  Connecting to API... ");
var healthy = await client.CheckHealthAsync();
if (!healthy)
{
    PrintError("API is unreachable. Make sure the Python FastAPI server is running.");
    PrintInfo($"  Start it with: uvicorn api.main:app --reload");
    Console.WriteLine();
    Console.Write("  Press Enter to continue in DEMO mode or Ctrl+C to exit: ");
    Console.ReadLine();
}
else
{
    PrintSuccess("Connected");
}

// ── Main Menu Loop ────────────────────────────────────────────
bool running = true;
while (running)
{
    PrintMenu();
    var choice = Console.ReadLine()?.Trim();
    Console.WriteLine();

    switch (choice)
    {
        case "1": await ShowEmployees(); break;
        case "2": await ShowDepartmentPayroll(); break;
        case "3": await ShowOvertimeReport(); break;
        case "4": await ShowValidationErrors(); break;
        case "5": await ShowTopOvertimeEmployees(); break;
        case "6": await ShowPayrollAnalytics(); break;
        case "0":
            PrintSuccess("Goodbye!");
            running = false;
            break;
        default:
            PrintWarning("Invalid option. Please enter 0-6.");
            break;
    }
}

// ─────────────────────────────────────────────────────────────
// MENU ACTIONS
// ─────────────────────────────────────────────────────────────

async Task ShowEmployees()
{
    PrintHeader("EMPLOYEE DIRECTORY");
    Console.Write("  Filter by department (or Enter to skip): ");
    var dept = Console.ReadLine()?.Trim();
    if (string.IsNullOrEmpty(dept)) dept = null;

    Console.Write("  Filter by status [Active/Inactive/On Leave] (or Enter to skip): ");
    var status = Console.ReadLine()?.Trim();
    if (string.IsNullOrEmpty(status)) status = null;

    var result = await client.GetEmployeesAsync(pageSize: 20, department: dept, status: status);
    if (result == null) return;

    Console.WriteLine($"\n  Total Employees: {result.Total:N0}  |  Showing: {result.Data.Count}");
    Console.WriteLine();
    PrintTableHeader("  ID          NAME                         DEPT               PAY/HR   STATUS");
    PrintDivider();
    foreach (var emp in result.Data)
    {
        Console.WriteLine($"  {emp.EmployeeId,-11} {emp.FullName,-30} {emp.Department,-18} ${emp.PayRate:F2,-7} {emp.Status}");
    }
    PrintDivider();
    Console.WriteLine();
    PressEnterToContinue();
}

async Task ShowDepartmentPayroll()
{
    PrintHeader("PAYROLL SUMMARY BY DEPARTMENT");
    var data = await client.GetDepartmentPayrollAsync();
    if (data == null || data.Count == 0) return;

    // Business logic: flag departments with >15% overtime rate
    var flagged = data.Where(d => d.OvertimeRatePct > 15).ToList();

    Console.WriteLine();
    PrintTableHeader("  DEPARTMENT          HEADCOUNT   HOURS        PAYROLL          OT%    FLAG");
    PrintDivider();
    foreach (var d in data)
    {
        var flag = d.OvertimeRatePct > 15 ? "⚠ HIGH OT" : "";
        Console.ForegroundColor = d.OvertimeRatePct > 15 ? ConsoleColor.Yellow : ConsoleColor.White;
        Console.WriteLine($"  {d.Department,-20} {d.TotalEmployees,-10} {d.TotalHours,-12:F1} ${d.TotalGrossPay,-15:N2} {d.OvertimeRatePct:F1}%   {flag}");
        Console.ResetColor();
    }
    PrintDivider();

    var totalPay   = data.Sum(d => d.TotalGrossPay);
    var totalHours = data.Sum(d => d.TotalHours);
    Console.WriteLine($"\n  TOTAL PAYROLL: ${totalPay:N2}  |  TOTAL HOURS: {totalHours:N1}h");
    
    if (flagged.Count > 0)
    {
        PrintWarning($"\n  ⚠ {flagged.Count} departments have overtime rate > 15%: {string.Join(", ", flagged.Select(f => f.Department))}");
    }
    Console.WriteLine();
    PressEnterToContinue();
}

async Task ShowOvertimeReport()
{
    PrintHeader("OVERTIME REPORT BY DEPARTMENT");
    var data = await client.GetOvertimeReportAsync();
    if (data == null || data.Count == 0) return;

    Console.WriteLine();
    PrintTableHeader("  DEPARTMENT          OT SHIFTS   OT HOURS     OT COST          RATE%");
    PrintDivider();
    foreach (var row in data.OrderByDescending(r => r.TotalOvertimeHours))
    {
        Console.ForegroundColor = row.OvertimeRatePct > 20 ? ConsoleColor.Red : ConsoleColor.White;
        Console.WriteLine(
            $"  {row.Department,-20} {row.OvertimeShifts,-10} {row.TotalOvertimeHours,-12:F1} ${row.TotalOvertimeCost,-15:N2} {row.OvertimeRatePct:F1}%"
        );
        Console.ResetColor();
    }
    PrintDivider();
    var totalOT    = data.Sum(d => d.TotalOvertimeHours);
    var totalOTCost = data.Sum(d => d.TotalOvertimeCost);
    Console.WriteLine($"\n  TOTAL OT HOURS: {totalOT:N1}h  |  TOTAL OT COST: ${totalOTCost:N2}");
    Console.WriteLine();
    PressEnterToContinue();
}

async Task ShowValidationErrors()
{
    PrintHeader("ETL VALIDATION ERRORS");
    Console.WriteLine("  Error types: MISSING_CLOCK_OUT | INVALID_CLOCK_SEQUENCE | DUPLICATE_RECORD | INVALID_HOURS");
    Console.Write("  Filter by type (or Enter to show all): ");
    var errorType = Console.ReadLine()?.Trim();
    if (string.IsNullOrEmpty(errorType)) errorType = null;

    var result = await client.GetValidationErrorsAsync(errorType: errorType, limit: 30);
    if (result == null) return;

    Console.WriteLine($"\n  Total Flagged: {result.Total:N0}  |  Showing: {result.Showing}");
    Console.WriteLine();
    PrintTableHeader("  LOG_ID          EMP_ID      ERROR TYPE                 FLAGGED AT");
    PrintDivider();
    foreach (var err in result.Errors)
    {
        Console.ForegroundColor = err.ErrorReason == "MISSING_CLOCK_OUT" ? ConsoleColor.Yellow : ConsoleColor.Red;
        Console.WriteLine($"  {(err.LogId ?? "N/A"),-16} {(err.EmployeeId ?? "N/A"),-12} {err.ErrorReason,-28} {err.FlaggedAt?[..19] ?? "N/A"}");
        Console.ResetColor();
    }
    PrintDivider();
    Console.WriteLine();
    PressEnterToContinue();
}

async Task ShowTopOvertimeEmployees()
{
    PrintHeader("TOP 25 OVERTIME EARNERS");
    var data = await client.GetTopOvertimeEmployeesAsync(25);
    if (data == null || data.Count == 0) return;

    Console.WriteLine();
    PrintTableHeader("  RANK   ID          NAME                       DEPT               OT HRS   OT COST");
    PrintDivider();
    int rank = 1;
    foreach (var emp in data)
    {
        Console.ForegroundColor = rank <= 3 ? ConsoleColor.Cyan : ConsoleColor.White;
        Console.WriteLine(
            $"  #{rank,-5} {emp.EmployeeId,-11} {emp.FullName,-28} {emp.Department,-18} {emp.TotalHours:F1,-8} ${emp.TotalGrossPay:N0}"
        );
        Console.ResetColor();
        rank++;
    }
    PrintDivider();
    Console.WriteLine();
    PressEnterToContinue();
}

async Task ShowPayrollAnalytics()
{
    PrintHeader("COMPANY PAYROLL ANALYTICS");
    var dept    = await client.GetDepartmentPayrollAsync();
    var overtime = await client.GetOvertimeReportAsync();
    if (dept == null || overtime == null) return;

    var totalPayroll    = dept.Sum(d => d.TotalGrossPay);
    var totalHours      = dept.Sum(d => d.TotalHours);
    var totalOT         = overtime.Sum(o => o.TotalOvertimeHours);
    var totalOTCost     = overtime.Sum(o => o.TotalOvertimeCost);
    var topDept         = dept.MaxBy(d => d.TotalGrossPay);
    var otRatePct       = totalHours > 0 ? totalOT / totalHours * 100 : 0;
    var otCostPct       = totalPayroll > 0 ? totalOTCost / totalPayroll * 100 : 0;

    Console.WriteLine();
    Console.WriteLine("  ┌─────────────────────────────────────────────┐");
    Console.WriteLine($"  │  TOTAL PAYROLL:    ${totalPayroll,22:N2}  │");
    Console.WriteLine($"  │  TOTAL HOURS:      {totalHours,22:N1}h │");
    Console.WriteLine($"  │  OVERTIME HOURS:   {totalOT,22:N1}h │");
    Console.WriteLine($"  │  OT RATE:          {otRatePct,22:F1}%  │");
    Console.WriteLine($"  │  OT COST:          ${totalOTCost,22:N2}  │");
    Console.WriteLine($"  │  OT % OF PAYROLL:  {otCostPct,22:F1}%  │");
    Console.WriteLine($"  │  COSTLIEST DEPT:   {topDept?.Department,22}  │");
    Console.WriteLine("  └─────────────────────────────────────────────┘");
    Console.WriteLine();
    PressEnterToContinue();
}

// ─────────────────────────────────────────────────────────────
// UI HELPERS
// ─────────────────────────────────────────────────────────────

void PrintBanner(string url)
{
    Console.Clear();
    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine();
    Console.WriteLine("  ╔══════════════════════════════════════════════════╗");
    Console.WriteLine("  ║   MTA ENTERPRISE WORKFORCE ADMIN DASHBOARD       ║");
    Console.WriteLine("  ║   C# Console Application  |  .NET 8              ║");
    Console.WriteLine("  ╚══════════════════════════════════════════════════╝");
    Console.ResetColor();
    Console.WriteLine($"  API: {url}");
    Console.WriteLine();
}

void PrintMenu()
{
    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine("  ┌─ MAIN MENU ─────────────────────────────────────┐");
    Console.WriteLine("  │  1  View Employees                               │");
    Console.WriteLine("  │  2  Department Payroll Summary                   │");
    Console.WriteLine("  │  3  Overtime Report                              │");
    Console.WriteLine("  │  4  ETL Validation Errors                        │");
    Console.WriteLine("  │  5  Top Overtime Earners                         │");
    Console.WriteLine("  │  6  Company Analytics Dashboard                  │");
    Console.WriteLine("  │  0  Exit                                         │");
    Console.WriteLine("  └─────────────────────────────────────────────────┘");
    Console.ResetColor();
    Console.Write("  Enter choice: ");
}

void PrintHeader(string title)
{
    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine($"\n  ═══ {title} ═══");
    Console.ResetColor();
}

void PrintTableHeader(string header)
{
    Console.ForegroundColor = ConsoleColor.DarkGray;
    Console.WriteLine(header);
    Console.ResetColor();
}

void PrintDivider() =>
    Console.WriteLine("  " + new string('─', 80));

void PrintSuccess(string msg) { Console.ForegroundColor = ConsoleColor.Green;  Console.WriteLine(msg); Console.ResetColor(); }
void PrintWarning(string msg) { Console.ForegroundColor = ConsoleColor.Yellow; Console.WriteLine(msg); Console.ResetColor(); }
void PrintError(string msg)   { Console.ForegroundColor = ConsoleColor.Red;    Console.WriteLine(msg); Console.ResetColor(); }
void PrintInfo(string msg)    { Console.ForegroundColor = ConsoleColor.Gray;   Console.WriteLine(msg); Console.ResetColor(); }
void PressEnterToContinue()   { Console.Write("  Press Enter to return to menu..."); Console.ReadLine(); Console.Clear(); PrintBanner(apiUrl); }
