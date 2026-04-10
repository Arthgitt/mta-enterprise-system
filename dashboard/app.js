/**
 * app.js — MTA Enterprise Workforce Analytics Dashboard
 * Fetches real data from the FastAPI backend, renders Chart.js charts
 * and populates the KPI cards + employee table.
 */

const API_BASE = window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
  ? "http://localhost:8000"
  : "";  // same origin if deployed on Vercel

// ─────────────────────────────────────────────────────────────
// CHART DEFAULTS
// ─────────────────────────────────────────────────────────────
Chart.defaults.color = "#8b949e";
Chart.defaults.borderColor = "rgba(255,255,255,0.06)";
Chart.defaults.font.family = "'Inter', system-ui, sans-serif";
Chart.defaults.font.size = 12;

const PALETTE = [
  "#4f9cf9","#7c3aed","#10b981","#f59e0b","#ef4444",
  "#06b6d4","#8b5cf6","#f97316","#14b8a6","#ec4899",
  "#84cc16","#a855f7","#0ea5e9","#d946ef","#22c55e"
];

// ─────────────────────────────────────────────────────────────
// UTILITIES
// ─────────────────────────────────────────────────────────────
async function fetchJSON(path) {
  try {
    const res = await fetch(`${API_BASE}${path}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (e) {
    console.warn(`API fetch failed [${path}]:`, e.message);
    return null;
  }
}

function fmt$  (n) { return n != null ? "$" + Number(n).toLocaleString("en-US", {minimumFractionDigits:0, maximumFractionDigits:0}) : "—"; }
function fmtN  (n) { return n != null ? Number(n).toLocaleString("en-US", {maximumFractionDigits:1}) : "—"; }
function fmtPct(n) { return n != null ? Number(n).toFixed(1) + "%" : "—"; }

// ─────────────────────────────────────────────────────────────
// LIVE CLOCK
// ─────────────────────────────────────────────────────────────
function startClock() {
  const el = document.getElementById("live-time");
  const tick = () => {
    el.textContent = new Date().toLocaleString("en-US", {
      weekday:"short", month:"short", day:"numeric",
      hour:"2-digit", minute:"2-digit", second:"2-digit"
    });
  };
  tick();
  setInterval(tick, 1000);
}

// ─────────────────────────────────────────────────────────────
// HEALTH CHECK
// ─────────────────────────────────────────────────────────────
async function checkHealth() {
  const badge = document.getElementById("health-badge");
  const data = await fetchJSON("/health");
  if (data && data.status === "healthy") {
    badge.textContent = "● API Connected";
    badge.className = "badge badge-healthy";
  } else {
    badge.textContent = "● API Unavailable";
    badge.className = "badge badge-degraded";
  }
}

// ─────────────────────────────────────────────────────────────
// CHART 1: PAYROLL BY DEPARTMENT (horizontal bar)
// ─────────────────────────────────────────────────────────────
async function renderDeptPayrollChart() {
  const data = await fetchJSON("/payroll/department-summary");
  if (!data) return;

  const sorted = [...data].sort((a, b) => b.total_gross_pay - a.total_gross_pay);
  const labels  = sorted.map(d => d.department);
  const payroll = sorted.map(d => d.total_gross_pay);
  const ot      = sorted.map(d => d.total_overtime_hours * (d.avg_pay_rate || 0) * 1.5);

  new Chart(document.getElementById("chart-dept-payroll"), {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Regular Pay",
          data: payroll.map((p, i) => p - ot[i]),
          backgroundColor: PALETTE[0] + "cc",
          borderRadius: 4,
        },
        {
          label: "Overtime Pay",
          data: ot,
          backgroundColor: PALETTE[4] + "cc",
          borderRadius: 4,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: { position: "top" },
        tooltip: {
          callbacks: {
            label: ctx => ` ${ctx.dataset.label}: ${fmt$(ctx.parsed.y)}`
          }
        }
      },
      scales: {
        x: { stacked: true },
        y: {
          stacked: true,
          ticks: { callback: v => fmt$(v) }
        }
      }
    }
  });

  // ── Update KPIs ──────────────────────────────────────────
  const totalPay   = data.reduce((s, d) => s + d.total_gross_pay, 0);
  const totalHours = data.reduce((s, d) => s + d.total_hours, 0);
  const totalOT    = data.reduce((s, d) => s + d.total_overtime_hours, 0);

  document.getElementById("kpi-total-payroll").textContent = fmt$(totalPay);
  document.getElementById("kpi-total-hours").textContent   = fmtN(totalHours) + "h";
  document.getElementById("kpi-overtime-pct").textContent  = fmtPct(totalOT / totalHours * 100);
  document.getElementById("kpi-depts").textContent         = data.length;
}

// ─────────────────────────────────────────────────────────────
// CHART 2: WEEKLY TREND (line chart)
// ─────────────────────────────────────────────────────────────
async function renderWeeklyTrendChart() {
  const data = await fetchJSON("/payroll/weekly-trend?weeks=12");
  if (!data) return;

  const labels     = data.map(w => w.week_start);
  const payroll    = data.map(w => w.weekly_payroll);
  const overtime   = data.map(w => w.weekly_overtime_cost);

  new Chart(document.getElementById("chart-weekly-trend"), {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Total Payroll",
          data: payroll,
          borderColor: PALETTE[0],
          backgroundColor: PALETTE[0] + "20",
          fill: true,
          tension: 0.4,
          pointRadius: 4,
          pointHoverRadius: 6,
        },
        {
          label: "Overtime Cost",
          data: overtime,
          borderColor: PALETTE[4],
          backgroundColor: PALETTE[4] + "20",
          fill: true,
          tension: 0.4,
          borderDash: [4, 4],
          pointRadius: 3,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: { position: "top" },
        tooltip: {
          callbacks: {
            label: ctx => ` ${ctx.dataset.label}: ${fmt$(ctx.parsed.y)}`
          }
        }
      },
      scales: {
        y: { ticks: { callback: v => fmt$(v) } }
      }
    }
  });
}

// ─────────────────────────────────────────────────────────────
// CHART 3: OVERTIME RATE BY DEPT (doughnut)
// ─────────────────────────────────────────────────────────────
async function renderOvertimeRateChart() {
  const data = await fetchJSON("/overtime-report");
  if (!data) return;

  const sorted = [...data].sort((a, b) => b.overtime_rate_pct - a.overtime_rate_pct).slice(0, 8);
  new Chart(document.getElementById("chart-ot-rate"), {
    type: "doughnut",
    data: {
      labels: sorted.map(d => d.department),
      datasets: [{
        data: sorted.map(d => d.overtime_rate_pct),
        backgroundColor: PALETTE.slice(0, sorted.length).map(c => c + "cc"),
        borderWidth: 1,
        borderColor: "#0d1117"
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: { position: "right", labels: { boxWidth: 12, padding: 8 } },
        tooltip: {
          callbacks: {
            label: ctx => ` ${ctx.label}: ${ctx.parsed.toFixed(1)}%`
          }
        }
      }
    }
  });
}

// ─────────────────────────────────────────────────────────────
// CHART 4: VALIDATION ERRORS (pie)
// ─────────────────────────────────────────────────────────────
async function renderErrorsChart() {
  const data = await fetchJSON("/validation-errors/summary");
  if (!data) return;

  document.getElementById("kpi-error-count").textContent = data.total_errors?.toLocaleString() ?? "—";
  const bd = data.breakdown || [];

  new Chart(document.getElementById("chart-errors"), {
    type: "pie",
    data: {
      labels: bd.map(e => e.error_reason.replace(/_/g, " ")),
      datasets: [{
        data: bd.map(e => e.count),
        backgroundColor: [PALETTE[4], PALETTE[2], PALETTE[3], PALETTE[1], PALETTE[0]].map(c => c + "cc"),
        borderWidth: 1,
        borderColor: "#0d1117"
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: { position: "right", labels: { boxWidth: 12, padding: 8 } },
        tooltip: {
          callbacks: {
            label: ctx => ` ${ctx.label}: ${ctx.parsed} (${bd[ctx.dataIndex]?.percentage}%)`
          }
        }
      }
    }
  });
}

// ─────────────────────────────────────────────────────────────
// TABLE: TOP 10 EMPLOYEES BY OVERTIME
// ─────────────────────────────────────────────────────────────
async function renderEmployeeTable() {
  const data = await fetchJSON("/overtime-report/employees?limit=10");
  const tbody = document.getElementById("tbody-employees");
  if (!data || data.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7" class="loading-cell">No data available. Run the ETL pipeline first.</td></tr>`;
    return;
  }

  const badgeClass = i => i === 0 ? "gold" : i === 1 ? "silver" : i === 2 ? "bronze" : "";
  tbody.innerHTML = data.map((emp, i) => `
    <tr>
      <td><span class="rank-badge ${badgeClass(i)}">${i + 1}</span></td>
      <td><code style="color:#4f9cf9;font-size:0.8rem">${emp.employee_id}</code></td>
      <td>${emp.full_name ?? (emp.first_name + " " + emp.last_name)}</td>
      <td>${emp.department}</td>
      <td>${fmtN(emp.total_hours)}h</td>
      <td style="color:${emp.total_overtime_hours > 0 ? '#f59e0b' : '#8b949e'}">${fmtN(emp.total_overtime_hours)}h</td>
      <td style="font-weight:600">${fmt$(emp.total_gross_pay ?? emp.total_overtime_cost)}</td>
    </tr>
  `).join("");
}

// ─────────────────────────────────────────────────────────────
// MAIN INIT
// ─────────────────────────────────────────────────────────────
async function init() {
  startClock();
  await checkHealth();

  // Load all data in parallel for fast render
  await Promise.allSettled([
    renderDeptPayrollChart(),
    renderWeeklyTrendChart(),
    renderOvertimeRateChart(),
    renderErrorsChart(),
    renderEmployeeTable(),
  ]);

  // Last refreshed timestamp
  document.getElementById("last-refreshed").textContent =
    "Last refreshed: " + new Date().toLocaleTimeString();
}

document.addEventListener("DOMContentLoaded", init);
