import pandas as pd
from transformlivedata.lineage.lineage_functions import (
    get_json_raw_fields_path_from_schema,
    build_api_lineage,
    build_join_lineage,
    merge_lineage_fragments,
    get_column_type,
)


# --- helpers ---


def make_schema():
    """Minimal schema with two levels of nesting and a $ref."""
    return {
        "properties": {
            "payload": {
                "properties": {
                    "lines": {
                        "type": "array",
                        "items": {
                            "properties": {
                                "line_code": {"type": "string"},
                                "vehicles": {
                                    "type": "array",
                                    "items": {"$ref": "#/definitions/Vehicle"},
                                },
                            }
                        },
                    }
                }
            }
        },
        "definitions": {
            "Vehicle": {
                "properties": {
                    "vehicle_id": {"type": "string"},
                    "lat": {"type": "number"},
                }
            }
        },
    }


# --- get_json_raw_fields_path_from_schema ---


def test_returns_first_level_field_paths():
    result = get_json_raw_fields_path_from_schema(make_schema())
    assert result["line_code"] == "payload.lines[i].line_code"


def test_returns_vehicle_field_paths():
    result = get_json_raw_fields_path_from_schema(make_schema())
    assert result["vehicle_id"] == "payload.lines[i].vehicles[j].vehicle_id"
    assert result["lat"] == "payload.lines[i].vehicles[j].lat"


def test_second_level_list_key_excluded_from_first_level():
    result = get_json_raw_fields_path_from_schema(make_schema())
    assert "vehicles" not in result


def test_returns_empty_dict_when_no_payload():
    result = get_json_raw_fields_path_from_schema({})
    assert result == {}


def test_returns_empty_dict_when_no_array_at_first_level():
    schema = {"properties": {"payload": {"properties": {"lines": {"type": "object"}}}}}
    result = get_json_raw_fields_path_from_schema(schema)
    assert result == {}


def test_returns_empty_dict_when_no_array_at_second_level():
    schema = {
        "properties": {
            "payload": {
                "properties": {
                    "lines": {
                        "type": "array",
                        "items": {
                            "properties": {
                                "line_code": {"type": "string"},
                            }
                        },
                    }
                }
            }
        }
    }
    result = get_json_raw_fields_path_from_schema(schema)
    assert result == {}


def test_returns_empty_dict_when_no_ref():
    schema = {
        "properties": {
            "payload": {
                "properties": {
                    "lines": {
                        "type": "array",
                        "items": {
                            "properties": {
                                "vehicles": {
                                    "type": "array",
                                    "items": {},
                                },
                            }
                        },
                    }
                }
            }
        }
    }
    result = get_json_raw_fields_path_from_schema(schema)
    assert result == {}


# --- build_api_lineage ---


def test_build_api_lineage_maps_rename():
    df = pd.DataFrame({"vehicle_id": pd.Series([], dtype=str)})
    result = build_api_lineage(
        df,
        rename_map={"raw_id": "vehicle_id"},
        raw_fields_path_map={"raw_id": "payload.lines[i].vehicles[j].raw_id"},
    )
    assert result["vehicle_id"]["inputs"] == ["payload.lines[i].vehicles[j].raw_id"]
    assert result["vehicle_id"]["transformation"] == "API rename/cast"


def test_build_api_lineage_falls_back_to_raw_key_when_not_in_path_map():
    df = pd.DataFrame({"vehicle_id": pd.Series([], dtype=str)})
    result = build_api_lineage(
        df,
        rename_map={"raw_id": "vehicle_id"},
        raw_fields_path_map={},
    )
    assert result["vehicle_id"]["inputs"] == ["raw_id"]


def test_build_api_lineage_empty_rename_map():
    df = pd.DataFrame({"vehicle_id": pd.Series([], dtype=str)})
    result = build_api_lineage(df, rename_map={}, raw_fields_path_map={})
    assert result == {}


# --- build_join_lineage ---


def test_build_join_lineage_maps_columns():
    df = pd.DataFrame(
        {"trip_id": pd.Series([], dtype=str), "route": pd.Series([], dtype=str)}
    )
    result = build_join_lineage(df, "trip_details", "trip_id", ["trip_id", "route"])
    assert "route" in result
    assert result["route"]["inputs"] == ["trip_details.route"]
    assert result["route"]["transformation"] == "trip_details left join"


def test_build_join_lineage_excludes_merge_key():
    df = pd.DataFrame(
        {"trip_id": pd.Series([], dtype=str), "route": pd.Series([], dtype=str)}
    )
    result = build_join_lineage(df, "trip_details", "trip_id", ["trip_id", "route"])
    assert "trip_id" not in result


def test_build_join_lineage_excludes_columns_not_in_df():
    df = pd.DataFrame({"route": pd.Series([], dtype=str)})
    result = build_join_lineage(
        df, "trip_details", "trip_id", ["trip_id", "route", "missing_col"]
    )
    assert "missing_col" not in result


def test_build_join_lineage_empty_columns():
    df = pd.DataFrame()
    result = build_join_lineage(df, "trip_details", "trip_id", [])
    assert result == {}


# --- merge_lineage_fragments ---


def test_merge_lineage_fragments_combines_dicts():
    a = {"col_a": {"inputs": ["x"]}}
    b = {"col_b": {"inputs": ["y"]}}
    result = merge_lineage_fragments(a, b)
    assert "col_a" in result
    assert "col_b" in result


def test_merge_lineage_fragments_later_wins_on_conflict():
    a = {"col": {"inputs": ["first"]}}
    b = {"col": {"inputs": ["second"]}}
    result = merge_lineage_fragments(a, b)
    assert result["col"]["inputs"] == ["second"]


def test_merge_lineage_fragments_no_args():
    result = merge_lineage_fragments()
    assert result == {}


# --- get_column_type ---


def test_get_column_type_int():
    df = pd.DataFrame({"x": pd.array([1, 2], dtype="int64")})
    assert get_column_type(df, "x") == "int"


def test_get_column_type_float():
    df = pd.DataFrame({"x": pd.array([1.0], dtype="float64")})
    assert get_column_type(df, "x") == "float"


def test_get_column_type_bool():
    df = pd.DataFrame({"x": pd.array([True, False], dtype="bool")})
    assert get_column_type(df, "x") == "boolean"


def test_get_column_type_datetime():
    df = pd.DataFrame({"x": pd.to_datetime(["2026-01-01"])})
    assert get_column_type(df, "x") == "timestamp"


def test_get_column_type_string():
    df = pd.DataFrame({"x": pd.array(["a", "b"], dtype="object")})
    assert get_column_type(df, "x") == "string"


def test_get_column_type_unknown_when_column_missing():
    df = pd.DataFrame({"x": [1]})
    assert get_column_type(df, "missing") == "unknown"
