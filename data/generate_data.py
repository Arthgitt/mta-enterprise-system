"""
generate_data.py
----------------
Generates realistic mock dataset for the MTA Enterprise Workforce System.
Creates:
  - employees.csv     : 1000 employee records
  - time_logs.csv     : 5000+ time log entries with intentional bad data

Run: python data/generate_data.py
"""

import csv
import os
import random
from datetime import datetime, timedelta

random.seed(42)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "raw")
NUM_EMPLOYEES = 1000
NUM_LOGS = 5200

DEPARTMENTS = [
    "Operations", "Maintenance", "Rail", "Bus", "Finance",
    "HR", "IT", "Safety", "Engineering", "Customer Service",
    "Scheduling", "Infrastructure", "Procurement", "Legal"
]

FIRST_NAMES = [
    "James", "Maria", "David", "Linda", "Robert", "Patricia",
    "Michael", "Jennifer", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
    "Christopher", "Nancy", "Daniel", "Lisa", "Paul", "Betty",
    "Mark", "Dorothy", "Donald", "Sandra", "George", "Ashley",
    "Kenneth", "Emily", "Steven", "Donna", "Edward", "Michelle",
    "Brian", "Carol", "Ronald", "Amanda", "Anthony", "Helen",
    "Kevin", "Melissa", "Jason", "Deborah", "Matthew", "Stephanie",
    "Gary", "Rebecca"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
    "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore",
    "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
    "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Walker", "Young", "Allen", "King", "Wright", "Scott",
    "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams",
    "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts"
]

STATUS_OPTIONS = ["Active", "Active", "Active", "Active", "Inactive", "On Leave"]


# ─────────────────────────────────────────────
# GENERATE EMPLOYEES
# ─────────────────────────────────────────────
def generate_employees(n: int) -> list[dict]:
    employees = []
    for i in range(1, n + 1):
        hire_date = datetime(2015, 1, 1) + timedelta(days=random.randint(0, 3650))
        employees.append({
            "employee_id": f"EMP{i:05d}",
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "department": random.choice(DEPARTMENTS),
            "pay_rate": round(random.uniform(18.50, 85.00), 2),
            "hire_date": hire_date.strftime("%Y-%m-%d"),
            "status": random.choice(STATUS_OPTIONS),
            "email": f"emp{i:05d}@mta-corp.org",
        })
    return employees


# ─────────────────────────────────────────────
# GENERATE TIME LOGS (with intentional bad data)
# ─────────────────────────────────────────────
def generate_time_logs(employees: list[dict], n: int) -> list[dict]:
    logs = []
    log_id = 1
    base_date = datetime(2024, 1, 1)

    active_employees = [e for e in employees if e["status"] == "Active"]

    for _ in range(n):
        emp = random.choice(active_employees)
        day_offset = random.randint(0, 364)
        log_date = base_date + timedelta(days=day_offset)
        shift_start_hour = random.choice([6, 7, 8, 9, 10, 14, 22])

        clock_in = log_date.replace(
            hour=shift_start_hour,
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )
        hours_worked = random.uniform(4.0, 14.0)
        clock_out = clock_in + timedelta(hours=hours_worked)

        # ── Introduce Bad Data ──────────────────────
        bad_dice = random.random()

        # ~5%: missing clock_out
        if bad_dice < 0.05:
            clock_out_str = ""
        # ~2%: clock_out before clock_in (invalid)
        elif bad_dice < 0.07:
            clock_out_str = (clock_in - timedelta(hours=random.uniform(1, 3))).strftime("%Y-%m-%d %H:%M:%S")
        else:
            clock_out_str = clock_out.strftime("%Y-%m-%d %H:%M:%S")

        logs.append({
            "log_id": f"LOG{log_id:07d}",
            "employee_id": emp["employee_id"],
            "clock_in": clock_in.strftime("%Y-%m-%d %H:%M:%S"),
            "clock_out": clock_out_str,
            "department": emp["department"],
            "pay_rate": emp["pay_rate"],
            "log_date": log_date.strftime("%Y-%m-%d"),
        })
        log_id += 1

    # ── Inject ~2% duplicates ───────────────────
    sample_size = int(n * 0.02)
    duplicates = random.sample(logs, sample_size)
    for dup in duplicates:
        logs.append(dup.copy())

    random.shuffle(logs)
    return logs


# ─────────────────────────────────────────────
# WRITE CSV
# ─────────────────────────────────────────────
def write_csv(filepath: str, rows: list[dict], fieldnames: list[str]) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✓ Written {len(rows):,} rows → {filepath}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("═" * 50)
    print("  MTA Enterprise — Mock Data Generator")
    print("═" * 50)

    print("\n[1/2] Generating employees...")
    employees = generate_employees(NUM_EMPLOYEES)
    write_csv(
        os.path.join(OUTPUT_DIR, "employees.csv"),
        employees,
        ["employee_id", "first_name", "last_name", "department", "pay_rate", "hire_date", "status", "email"]
    )

    print("\n[2/2] Generating time logs...")
    logs = generate_time_logs(employees, NUM_LOGS)
    write_csv(
        os.path.join(OUTPUT_DIR, "time_logs.csv"),
        logs,
        ["log_id", "employee_id", "clock_in", "clock_out", "department", "pay_rate", "log_date"]
    )

    print(f"\n✅ Dataset ready in: {OUTPUT_DIR}/")
    print(f"   Employees : {NUM_EMPLOYEES:,}")
    print(f"   Time Logs : {len(logs):,} (incl. bad data + duplicates)")
    print("═" * 50)
