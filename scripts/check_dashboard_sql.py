"""Run every dataset query in a .lvdash.json against a SQL warehouse and report row counts.

Usage: python3 scripts/check_dashboard_sql.py <warehouse_id> [catalog] [schema]
Uses the Databricks CLI's current auth profile.
"""

import json
import subprocess
import sys
from pathlib import Path

DASHBOARD = Path(__file__).parent.parent / "src/dashboards/faers_radar.lvdash.json"


def main() -> int:
    warehouse = sys.argv[1]
    catalog = sys.argv[2] if len(sys.argv) > 2 else "workspace"
    schema = sys.argv[3] if len(sys.argv) > 3 else "faers"

    failed = 0
    for ds in json.loads(DASHBOARD.read_text())["datasets"]:
        body = {
            "warehouse_id": warehouse,
            "catalog": catalog,
            "schema": schema,
            "wait_timeout": "50s",
            "statement": "".join(ds["queryLines"]),
        }
        out = subprocess.run(
            ["databricks", "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body)],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        resp = json.loads(out)
        state = resp["status"]["state"]
        if state != "SUCCEEDED":
            failed += 1
            print(f"FAIL {ds['name']}: {resp['status'].get('error', {}).get('message', state)}")
            continue
        rows = resp.get("result", {}).get("data_array", [])
        print(f"ok   {ds['name']}: {resp['manifest'].get('total_row_count')} rows")
        for row in rows[:3]:
            print(f"       {row}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
