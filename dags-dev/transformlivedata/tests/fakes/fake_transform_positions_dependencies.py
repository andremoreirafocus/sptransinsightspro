from transformlivedata.services.transform_positions import (
    TransformPositionsDependencies,
    get_transform_positions_dependencies,
)
from typing import Any, cast


class FakeTransformPositionsDependencies:
    @staticmethod
    def create(**overrides) -> TransformPositionsDependencies:
        base = get_transform_positions_dependencies()
        return TransformPositionsDependencies(
            get_json_raw_fields_path_from_schema=cast(
                Any,
                overrides.get(
                    "get_json_raw_fields_path_from_schema",
                    base.get_json_raw_fields_path_from_schema,
                ),
            ),
            load_trip_details=cast(
                Any,
                overrides.get("load_trip_details", base.load_trip_details),
            ),
            flatten_raw_positions=cast(
                Any,
                overrides.get("flatten_raw_positions", base.flatten_raw_positions),
            ),
            normalize_columns=cast(
                Any,
                overrides.get("normalize_columns", base.normalize_columns),
            ),
            enrich_with_trip_details=cast(
                Any,
                overrides.get("enrich_with_trip_details", base.enrich_with_trip_details),
            ),
            compute_distances=cast(
                Any,
                overrides.get("compute_distances", base.compute_distances),
            ),
        )
