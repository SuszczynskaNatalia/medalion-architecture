"""
export_to_csv.py
----------------
Exports Gold-layer Snowflake tables to CSV files under data/exports/.
Run once after every pipeline execution to keep committed snapshots up-to-date.

Usage (from project root):
    python scripts/export_to_csv.py

Requirements: snowflake-connector-python, python-dotenv (already in requirements.txt)
"""

import csv
import os
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────
# W Dockerze skrypt jest pod /opt/airflow/scripts/, .env w /opt/airflow/
# Lokalnie skrypt jest pod scripts/, .env w katalogu projektu
_env_candidates = [
    Path(__file__).resolve().parent.parent / ".env",  # lokalnie
    Path("/opt/airflow/.env"),                         # Docker
]
for _env in _env_candidates:
    if _env.exists():
        load_dotenv(_env)
        break

CONN = dict(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE", "TAXI_PROJECT"),
)

# Lokalnie: <repo>/data/exports/  |  Docker: /opt/airflow/data/exports/
_data_root = (
    Path("/opt/airflow/data")
    if Path("/opt/airflow/data").exists()
    else Path(__file__).resolve().parent.parent / "data"
)
OUT_DIR = _data_root / "exports"

# Tables to export: (schema, table, output_filename)
EXPORTS = [
    ("GOLD", "REVENUE_BY_ZONE",           "revenue_by_zone.csv"),
    ("GOLD", "REVENUE_BY_VENDOR_PAYMENT", "revenue_by_vendor_payment.csv"),
    ("GOLD", "DQ_METRICS",               "dq_metrics_latest.csv"),
]

# For DQ_METRICS export only the latest run snapshot
CUSTOM_SQL = {
    "DQ_METRICS": """
        SELECT metric_name, current_value, unit, threshold, status, run_at
        FROM GOLD.DQ_METRICS
        WHERE run_at = (SELECT MAX(run_at) FROM GOLD.DQ_METRICS)
        ORDER BY metric_name
    """,
}

# ── Main ──────────────────────────────────────────────────────────────────────

def export_table(cur, schema: str, table: str, out_path: Path) -> int:
    sql = CUSTOM_SQL.get(table, f"SELECT * FROM {schema}.{table}")
    cur.execute(sql)
    rows = cur.fetchall()
    headers = [d[0] for d in cur.description]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    return len(rows)


def main() -> None:
    print(f"Connecting to Snowflake ({CONN['account']})…")
    conn = snowflake.connector.connect(**CONN)
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {CONN['database']}")

    exported_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    summary = []

    for schema, table, filename in EXPORTS:
        out_path = OUT_DIR / filename
        try:
            n = export_table(cur, schema, table, out_path)
            summary.append((filename, n, "OK"))
            print(f"  ✓  {filename}  ({n} rows)")
        except Exception as exc:
            summary.append((filename, 0, f"ERROR: {exc}"))
            print(f"  ✗  {filename}  — {exc}")

    # Write a tiny manifest so consumers know when the export was generated
    manifest_path = OUT_DIR / "_export_manifest.txt"
    with open(manifest_path, "w", encoding="utf-8") as f:
        f.write(f"exported_at: {exported_at}\n")
        f.write(f"source: Snowflake / {CONN['database']} / GOLD\n\n")
        for fname, n, status in summary:
            f.write(f"{fname}: {n} rows  [{status}]\n")

    cur.close()
    conn.close()
    print(f"\nDone. Files saved to: {OUT_DIR}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
