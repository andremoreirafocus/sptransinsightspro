from dataclasses import dataclass, field

from updatelatestpositions.orchestration_dependencies import (
    UpdateLatestPositionsOrchestrationDependencies,
)


@dataclass
class OrchestrationCallRecorder:
    create_calls: list = field(default_factory=list)


class FakeUpdateLatestPositionsOrchestrationDependencies:
    @classmethod
    def create_scenario(
        cls,
        *,
        config_raises: Exception | None = None,
        create_raises: Exception | None = None,
    ) -> tuple[UpdateLatestPositionsOrchestrationDependencies, OrchestrationCallRecorder]:
        recorder = OrchestrationCallRecorder()

        def get_config(*args, **kwargs):
            if config_raises is not None:
                raise config_raises
            return {"fake": "config"}

        def create_latest_positions(config):
            recorder.create_calls.append(config)
            if create_raises is not None:
                raise create_raises

        deps = UpdateLatestPositionsOrchestrationDependencies(
            get_config=get_config,
            create_latest_positions=create_latest_positions,
        )
        return deps, recorder
