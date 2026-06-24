from refinedtripfacts.orchestration_dependencies import (
    get_refinedtripfacts_orchestration_dependencies,
)


def test_dependencies_dataclass_exposes_all_required_services():
    deps = get_refinedtripfacts_orchestration_dependencies()
    assert callable(deps.get_config)
    assert callable(deps.measure_input_trips)
    assert callable(deps.provision_dim_time)
    assert callable(deps.create_trip_facts)
    assert callable(deps.measure_persisted_facts)
    assert callable(deps.get_trip_facts_table_columns)
    assert callable(deps.validate_trip_facts_quality)
    assert callable(deps.create_final_quality_report)
    assert callable(deps.create_failure_quality_report)
