#!/usr/bin/env python3
import json
import sys


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: check_airflow_dag_exists.py <dag_id>", file=sys.stderr)
        return 2

    dag_id = sys.argv[1]
    dags = json.load(sys.stdin)
    return 0 if any(d.get("dag_id") == dag_id for d in dags) else 1


if __name__ == "__main__":
    raise SystemExit(main())
