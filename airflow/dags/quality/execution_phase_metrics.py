from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
from time import perf_counter
from typing import Any, Dict, List


@dataclass
class ExecutionPhaseMetricsTracker:
    pipeline: str
    execution_id: str
    logical_date_utc: str
    phase_order: List[str]
    _execution_start: float = field(default_factory=perf_counter, init=False)
    _phase_start_times: Dict[str, float] = field(default_factory=dict, init=False)
    _phase_metrics: Dict[str, Dict[str, Any]] = field(init=False)

    def __post_init__(self) -> None:
        self._phase_metrics = {
            phase: {"duration_seconds": 0.0, "status": "skipped"}
            for phase in self.phase_order
        }

    def begin(self, phase_name: str) -> None:
        self._phase_start_times[phase_name] = perf_counter()

    def finish(self, phase_name: str, status: str) -> None:
        started_at = self._phase_start_times.get(phase_name)
        duration = 0.0
        if started_at is not None:
            duration = perf_counter() - started_at
        self._phase_metrics[phase_name] = {
            "duration_seconds": round(duration, 6),
            "status": status,
        }

    def to_log_payload(self, overall_status: str) -> Dict[str, Any]:
        return {
            "event": "execution_phase_metrics",
            "pipeline": self.pipeline,
            "execution_id": self.execution_id,
            "logical_date_utc": self.logical_date_utc,
            "overall_status": overall_status,
            "total_duration_seconds": round(perf_counter() - self._execution_start, 6),
            "phase_metrics": self._phase_metrics,
        }

    def emit(
        self,
        logger: logging.Logger,
        overall_status: str,
        ensure_ascii: bool = True,
    ) -> None:
        payload = self.to_log_payload(overall_status)
        message = json.dumps(payload, ensure_ascii=ensure_ascii)
        if overall_status == "success":
            logger.info(message)
        else:
            logger.error(message)
