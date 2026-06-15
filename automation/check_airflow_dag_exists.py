#!/usr/bin/env python3
import json
import sys


def _extract_json_payload(raw: str):
    for start_char in ("[", "{"):
        start = raw.find(start_char)
        if start == -1:
            continue
        try:
            return json.loads(raw[start:])
        except json.JSONDecodeError:
            continue
    raise ValueError("No valid JSON payload found in input stream.")


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: check_airflow_dag_exists.py <dag_id>", file=sys.stderr)
        return 2

    dag_id = sys.argv[1]
    raw = sys.stdin.read()
    dags = _extract_json_payload(raw)
    return 0 if any(d.get("dag_id") == dag_id for d in dags) else 1


if __name__ == "__main__":
    raise SystemExit(main())
