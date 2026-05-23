from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd

from refinedfinishedtrips.orchestration_dependencies import (
    RefinedFinishedTripsOrchestrationDependencies,
)

_BASE_TS = datetime(2026, 4, 14, 10, 0, 0, tzinfo=timezone.utc)


@dataclass
class OrchestrationCallRecorder:
    save_calls: list = field(default_factory=list)
    failure_report_calls: list = field(default_factory=list)
    final_report_calls: list = field(default_factory=list)


class FakeRefinedFinishedTripsOrchestrationDependencies:
    @staticmethod
    def _default_positions_df() -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "veiculo_ts": _BASE_TS,
                    "extracao_ts": _BASE_TS,
                    "linha_lt": "1234-10",
                    "veiculo_id": 100,
                    "linha_sentido": 1,
                    "is_circular": False,
                }
            ]
        )

    @classmethod
    def create_scenario(
        cls,
        *,
        positions_status: str = "PASS",
        extract_trips_output=None,
        extract_trips_raises: Exception | None = None,
        save_trips_raises: Exception | None = None,
        final_report_raises: Exception | None = None,
    ) -> tuple[RefinedFinishedTripsOrchestrationDependencies, OrchestrationCallRecorder]:
        recorder = OrchestrationCallRecorder()

        def get_config(pipeline_name_or_config, *args, **kwargs):
            if isinstance(pipeline_name_or_config, dict):
                return pipeline_name_or_config
            return {}

        def get_recent_positions(config):
            return cls._default_positions_df()

        def validate_positions_quality(config, df):
            if positions_status == "FAIL":
                return {
                    "status": "FAIL",
                    "positions_in_time_window_count": len(df),
                    "checks": [
                        {
                            "check": "freshness",
                            "status": "FAIL",
                            "reason": "no positions available for the analysis time window",
                        }
                    ],
                }
            if positions_status == "WARN":
                return {
                    "status": "WARN",
                    "positions_in_time_window_count": len(df),
                    "checks": [
                        {
                            "check": "freshness",
                            "status": "WARN",
                            "reason": "freshness lag above warning threshold",
                        }
                    ],
                }
            return {"status": "PASS", "positions_in_time_window_count": len(df), "checks": []}

        def get_all_finished_trips(config, df):
            if extract_trips_raises is not None:
                raise extract_trips_raises
            return extract_trips_output if extract_trips_output is not None else ([], {})

        def validate_trips_quality(config, df, trips, extraction_metrics):
            metrics = extraction_metrics or {}
            return {
                "status": "PASS",
                "effective_window_minutes": 0.0,
                "trips_extracted": len(trips) if isinstance(trips, list) else 0,
                "source_sentido_discrepancies": metrics.get("total_source_sentido_discrepancies", 0),
                "sanitization_dropped_points": metrics.get("total_input_position_sanitization_drops", 0),
                "input_position_records": metrics.get("total_input_position_records", len(df)),
                "vehicle_line_groups_processed": metrics.get("vehicle_line_groups_processed", 0),
                "checks": [],
            }

        def save_finished_trips_to_db(config, trips):
            recorder.save_calls.append(trips)
            if save_trips_raises is not None:
                raise save_trips_raises
            return {"added_rows": len(trips), "previously_saved_rows": 0}

        def create_failure_quality_report(
            config,
            execution_id,
            run_ts,
            failure_phase,
            failure_message,
            positions_result,
            trips_result=None,
            persistence_result=None,
            column_lineage=None,
            write_fn=None,
        ):
            recorder.failure_report_calls.append(
                {
                    "failure_phase": failure_phase,
                    "failure_message": failure_message,
                    "positions_result": positions_result,
                    "trips_result": trips_result,
                    "persistence_result": persistence_result,
                    "column_lineage": column_lineage,
                }
            )
            return {"summary": {"status": "FAIL"}, "details": {}}

        def create_final_quality_report(
            config,
            execution_id,
            run_ts,
            positions_result,
            trips_result,
            persistence_result,
            column_lineage=None,
            write_fn=None,
        ):
            recorder.final_report_calls.append(
                {
                    "positions_result": positions_result,
                    "trips_result": trips_result,
                    "persistence_result": persistence_result,
                    "column_lineage": column_lineage,
                }
            )
            if final_report_raises is not None:
                raise final_report_raises
            return {"summary": {"status": "PASS", "execution_id": execution_id}, "details": {}}

        deps = RefinedFinishedTripsOrchestrationDependencies(
            get_config=get_config,
            get_recent_positions=get_recent_positions,
            get_all_finished_trips=get_all_finished_trips,
            validate_positions_quality=validate_positions_quality,
            validate_trips_quality=validate_trips_quality,
            save_finished_trips_to_db=save_finished_trips_to_db,
            create_failure_quality_report=create_failure_quality_report,
            create_final_quality_report=create_final_quality_report,
        )
        return deps, recorder
