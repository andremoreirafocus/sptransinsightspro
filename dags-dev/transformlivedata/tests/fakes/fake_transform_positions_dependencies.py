from transformlivedata.services.transform_positions import (
    TransformPositionsDependencies,
    get_transform_positions_dependencies,
)


class FakeTransformPositionsDependencies:
    @staticmethod
    def create(**overrides) -> TransformPositionsDependencies:
        base = get_transform_positions_dependencies()
        values = {
            "get_json_raw_fields_path_from_schema": base.get_json_raw_fields_path_from_schema,
            "load_trip_details": base.load_trip_details,
            "flatten_raw_positions": base.flatten_raw_positions,
            "normalize_columns": base.normalize_columns,
            "enrich_with_trip_details": base.enrich_with_trip_details,
            "compute_distances": base.compute_distances,
        }
        values.update(overrides)
        return TransformPositionsDependencies(**values)
