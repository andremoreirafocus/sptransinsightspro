from __future__ import annotations

import json
import logging
from time import perf_counter
from typing import Any, Dict, List


def ensure_tracker_context(run_context: Dict[str, Any], phase_order: List[str]) -> Dict[str, Any]:
    tracker = run_context.get("execution_phase_metrics")
    if isinstance(tracker, dict):
        return tracker
    tracker = {
        "execution_start": perf_counter(),
        "phase_metrics": {
            phase: {"duration_seconds": 0.0, "status": "skipped"}
            for phase in phase_order
        },
        "phase_starts": {},
    }
    run_context["execution_phase_metrics"] = tracker
    return tracker


def begin_phase(run_context: Dict[str, Any], phase_order: List[str], phase_name: str) -> None:
    tracker = ensure_tracker_context(run_context, phase_order)
    phase_starts = tracker.setdefault("phase_starts", {})
    phase_starts[phase_name] = perf_counter()


def finish_phase(
    run_context: Dict[str, Any],
    phase_order: List[str],
    phase_name: str,
    status: str,
) -> None:
    tracker = ensure_tracker_context(run_context, phase_order)
    started_at = tracker.get("phase_starts", {}).get(phase_name)
    duration = 0.0
    if started_at is not None:
        duration = perf_counter() - started_at
    tracker["phase_metrics"][phase_name] = {
        "duration_seconds": round(duration, 6),
        "status": status,
    }


def emit_phase_metrics(
    run_context: Dict[str, Any],
    phase_order: List[str],
    logger: logging.Logger,
    pipeline: str,
    logical_date_utc: str,
    overall_status: str,
    ensure_ascii: bool = True,
) -> None:
    tracker = ensure_tracker_context(run_context, phase_order)
    payload = {
        "event": "execution_phase_metrics",
        "pipeline": pipeline,
        "execution_id": run_context["execution_id"],
        "logical_date_utc": logical_date_utc,
        "overall_status": overall_status,
        "total_duration_seconds": round(
            perf_counter() - tracker["execution_start"], 6
        ),
        "phase_metrics": tracker["phase_metrics"],
    }
    message = json.dumps(payload, ensure_ascii=ensure_ascii)
    if overall_status == "success":
        logger.info(message)
    else:
        logger.error(message)
