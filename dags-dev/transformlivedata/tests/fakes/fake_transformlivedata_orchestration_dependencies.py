from dataclasses import dataclass, field

import pandas as pd

from transformlivedata.orchestration_dependencies import (
    TransformLiveDataOrchestrationDependencies,
)


@dataclass
class OrchestrationCallRecorder:
    calls: list[str] = field(default_factory=list)
    save_calls: list[str] = field(default_factory=list)
    mark_processed_calls: int = 0
    quality_report_calls: int = 0
    failure_quality_report_calls: int = 0


class FakeTransformLiveDataOrchestrationDependencies:
    @staticmethod
    def _base_config():
        return {
            "general": {
                "notifications": {"webhook_url": "disabled"},
                "storage": {
                    "trusted_bucket": "trusted",
                    "quarantined_bucket": "quarantined",
                    "metadata_bucket": "metadata",
                    "quality_report_folder": "quality-reports",
                    "app_folder": "sptrans",
                },
                "tables": {"positions_table_name": "positions"},
            },
            "connections": {"object_storage": {"endpoint": "minio:9000"}},
            "raw_data_json_schema": {"dummy": True},
            "data_expectations": {"dummy": True},
        }

    @staticmethod
    def _valid_positions_df():
        return pd.DataFrame(
            [{"extracao_ts": "2026-05-17T10:00:00+00:00", "veiculo_id": 1}]
        )

    @classmethod
    def create_scenario(
        cls,
        *,
        load_raises: Exception | None = None,
        schema_valid: bool = True,
        transform_positions_df: pd.DataFrame | None = None,
        transform_invalid_df: pd.DataFrame | None = None,
        expectations_raises: Exception | None = None,
        expectations_invalid_df: pd.DataFrame | None = None,
        save_trusted_raises: Exception | None = None,
        save_quarantined_raises: Exception | None = None,
        mark_processed_raises: Exception | None = None,
        quality_report_raises: Exception | None = None,
    ) -> tuple[TransformLiveDataOrchestrationDependencies, OrchestrationCallRecorder]:
        recorder = OrchestrationCallRecorder()
        valid_positions_df = (
            transform_positions_df
            if transform_positions_df is not None
            else cls._valid_positions_df()
        )
        invalid_positions_df = (
            transform_invalid_df if transform_invalid_df is not None else pd.DataFrame()
        )
        expectations_invalid = (
            expectations_invalid_df
            if expectations_invalid_df is not None
            else pd.DataFrame()
        )

        def build_logical_date_context(_):
            recorder.calls.append("build_logical_date_context")
            return {
                "partition_path": "year=2026/month=05/day=17/",
                "source_file": "posicoes_onibus-202605170700.json",
            }

        def get_config(*args, **kwargs):
            recorder.calls.append("get_config")
            return cls._base_config()

        def load_positions(*args, **kwargs):
            recorder.calls.append("load_positions")
            if load_raises is not None:
                raise load_raises
            return {"metadata": {}, "payload": {"l": []}}

        def validate_json_data_schema(raw, schema):
            recorder.calls.append("validate_json_data_schema")
            if schema_valid:
                return True, []
            return False, ["schema error"]

        def transform_positions(*args, **kwargs):
            recorder.calls.append("transform_positions")
            return {
                "positions": valid_positions_df,
                "invalid_positions": invalid_positions_df,
                "batch_ts": "2026-05-17T10:00:00+00:00",
            }

        def validate_expectations(*args, **kwargs):
            recorder.calls.append("validate_expectations")
            if expectations_raises is not None:
                raise expectations_raises
            return {"valid_df": valid_positions_df, "invalid_df": expectations_invalid}

        def save_positions_to_storage(config, positions_df, target_bucket):
            recorder.calls.append(f"save_positions_to_storage:{target_bucket}")
            recorder.save_calls.append(target_bucket)
            if target_bucket == "trusted" and save_trusted_raises is not None:
                raise save_trusted_raises
            if target_bucket == "quarantined" and save_quarantined_raises is not None:
                raise save_quarantined_raises

        def mark_request_as_processed(*args, **kwargs):
            recorder.calls.append("mark_request_as_processed")
            recorder.mark_processed_calls += 1
            if mark_processed_raises is not None:
                raise mark_processed_raises
            return True

        def create_data_quality_report(*args, **kwargs):
            recorder.calls.append("create_data_quality_report")
            recorder.quality_report_calls += 1
            if quality_report_raises is not None:
                raise quality_report_raises
            return {
                "summary_status": "PASS",
                "quality_report_path": "metadata/quality/report.json",
                "record_counts": {},
                "transformation_processing_metrics": {},
                "transformation_processing_issues": {},
                "post_transformation_validation_summary": {},
            }

        def create_failure_quality_report(*args, **kwargs):
            recorder.calls.append("create_failure_quality_report")
            recorder.failure_quality_report_calls += 1
            return {
                "summary_status": "FAIL",
                "quality_report_path": "metadata/quality/report.json",
                "failure_phase": kwargs.get("failure_phase", ""),
                "failure_message": kwargs.get("failure_message", ""),
                "record_counts": {},
                "transformation_processing_metrics": {},
                "transformation_processing_issues": {},
                "post_transformation_validation_summary": {},
            }

        deps = TransformLiveDataOrchestrationDependencies(
            build_logical_date_context=build_logical_date_context,
            get_config=get_config,
            load_positions=load_positions,
            validate_json_data_schema=validate_json_data_schema,
            transform_positions=transform_positions,
            validate_expectations=validate_expectations,
            save_positions_to_storage=save_positions_to_storage,
            mark_request_as_processed=mark_request_as_processed,
            create_data_quality_report=create_data_quality_report,
            create_failure_quality_report=create_failure_quality_report,
        )
        return deps, recorder
