from dataclasses import dataclass, field

import pandas as pd

from refinedsynctripdetails.orchestration_dependencies import (
    RefinedSyncTripDetailsOrchestrationDependencies,
)


@dataclass
class OrchestrationCallRecorder:
    load_calls: list = field(default_factory=list)
    transform_calls: list = field(default_factory=list)
    save_calls: list = field(default_factory=list)


class FakeRefinedSyncTripDetailsOrchestrationDependencies:
    @classmethod
    def create_scenario(
        cls,
        *,
        config_raises: Exception | None = None,
        load_raises: Exception | None = None,
        transform_raises: Exception | None = None,
        save_raises: Exception | None = None,
    ) -> tuple[RefinedSyncTripDetailsOrchestrationDependencies, OrchestrationCallRecorder]:
        recorder = OrchestrationCallRecorder()

        def get_config(*args, **kwargs):
            if config_raises is not None:
                raise config_raises
            return {"fake": "config"}

        def load_trip_details(config):
            recorder.load_calls.append(config)
            if load_raises is not None:
                raise load_raises
            return pd.DataFrame({"trip_id": ["t1"]})

        def transform_trip_details(df):
            recorder.transform_calls.append(df)
            if transform_raises is not None:
                raise transform_raises
            return df

        def save_trip_details(config, df):
            recorder.save_calls.append((config, df))
            if save_raises is not None:
                raise save_raises

        deps = RefinedSyncTripDetailsOrchestrationDependencies(
            get_config=get_config,
            load_trip_details=load_trip_details,
            transform_trip_details=transform_trip_details,
            save_trip_details=save_trip_details,
        )
        return deps, recorder
