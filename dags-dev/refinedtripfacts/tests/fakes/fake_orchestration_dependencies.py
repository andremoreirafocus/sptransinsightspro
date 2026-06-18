from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from refinedtripfacts.orchestration_dependencies import RefinedTripFactsOrchestrationDependencies


def _make_config() -> Dict[str, Any]:
    return {
        "general": {
            "tables": {
                "finished_trips_table_name": "refined.finished_trips",
                "trip_facts_table_name": "refined.trip_facts",
                "dim_time_table_name": "refined.dim_time",
            },
            "quality": {
                "completeness_loss_rate_warn_threshold": 0.01,
                "completeness_loss_rate_fail_threshold": 0.05,
                "avg_speed_kmh_max": 120.0,
            },
            "storage": {
                "metadata_bucket": "metadata",
                "quality_report_folder": "quality-reports",
            },
        },
        "connections": {
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_sptrans",
                "user": "postgres",
                "password": "postgres",
            },
            "object_storage": {
                "endpoint": "localhost:9000",
                "access_key": "minioadmin",
                "secret_key": "minioadmin",
            },
        },
    }


def _make_quality_result(status: str = "PASS") -> Dict[str, Any]:
    return {
        "status": status,
        "loss_rate": 0.0,
        "persisted_facts": 10,
        "finished_trips_read": 10,
        "checks": [
            {"check": "completeness", "status": status},
            {"check": "dim_time_coverage", "status": "PASS", "uncovered_dim_keys": 0},
            {"check": "value_domain", "status": "PASS"},
        ],
    }


def _make_report(status: str = "PASS") -> Dict[str, Any]:
    return {
        "summary": {"status": status, "drift_detected": False, "execution_id": "fake-exec-id"},
        "details": {},
    }


class FakeOrchestrationDependencies:
    def __init__(
        self,
        finished_trips_read: int = 10,
        config_raises: Optional[Exception] = None,
        measurement_raises: Optional[Exception] = None,
        provision_raises: Optional[Exception] = None,
        creation_raises: Optional[Exception] = None,
        verification_raises: Optional[Exception] = None,
        lineage_raises: Optional[Exception] = None,
        quality_status: str = "PASS",
    ):
        self.call_log: List[str] = []
        self.call_kwargs: Dict[str, Any] = {}
        self._finished_trips_read = finished_trips_read
        self._config_raises = config_raises
        self._measurement_raises = measurement_raises
        self._provision_raises = provision_raises
        self._creation_raises = creation_raises
        self._verification_raises = verification_raises
        self._lineage_raises = lineage_raises
        self._quality_status = quality_status

    def _record(self, name: str, **kwargs: Any) -> None:
        self.call_log.append(name)
        self.call_kwargs[name] = kwargs

    def get_config(self, *args, **kwargs):
        self._record("get_config")
        if self._config_raises:
            raise self._config_raises
        return _make_config()

    def measure_input_trips(self, logic_date, config):
        self._record("measure_input_trips", logic_date=logic_date)
        if self._measurement_raises:
            raise self._measurement_raises
        return {"finished_trips_read": self._finished_trips_read}

    def provision_dim_time(self, logic_date, config):
        self._record("provision_dim_time", logic_date=logic_date)
        if self._provision_raises:
            raise self._provision_raises
        return {"rows_ensured": 24}

    def create_trip_facts(self, logic_date, config):
        self._record("create_trip_facts", logic_date=logic_date)
        if self._creation_raises:
            raise self._creation_raises
        return {
            "facts_derived": self._finished_trips_read,
            "inserted_rows": self._finished_trips_read,
            "skipped_rows": 0,
        }

    def measure_persisted_facts(self, logic_date, config):
        self._record("measure_persisted_facts", logic_date=logic_date)
        if self._verification_raises:
            raise self._verification_raises
        return {
            "persisted_facts": self._finished_trips_read,
            "uncovered_dim_keys": 0,
            "negative_duration": 0,
            "negative_distance": 0,
            "time_incoherent": 0,
            "implausible_speed": 0,
        }

    def get_trip_facts_table_columns(self, config):
        self._record("get_trip_facts_table_columns")
        if self._lineage_raises:
            raise self._lineage_raises
        return []

    def validate_trip_facts_quality(self, config, finished_trips_read, persisted_metrics):
        self._record(
            "validate_trip_facts_quality",
            finished_trips_read=finished_trips_read,
            persisted_metrics=persisted_metrics,
        )
        return _make_quality_result(self._quality_status)

    def create_final_quality_report(self, **kwargs):
        self._record("create_final_quality_report", **kwargs)
        return _make_report(self._quality_status)

    def create_failure_quality_report(self, **kwargs):
        self._record("create_failure_quality_report", **kwargs)
        return _make_report("FAIL")

    def as_deps(self) -> RefinedTripFactsOrchestrationDependencies:
        return RefinedTripFactsOrchestrationDependencies(
            get_config=self.get_config,
            measure_input_trips=self.measure_input_trips,
            provision_dim_time=self.provision_dim_time,
            create_trip_facts=self.create_trip_facts,
            measure_persisted_facts=self.measure_persisted_facts,
            get_trip_facts_table_columns=self.get_trip_facts_table_columns,
            validate_trip_facts_quality=self.validate_trip_facts_quality,
            create_final_quality_report=self.create_final_quality_report,
            create_failure_quality_report=self.create_failure_quality_report,
        )
