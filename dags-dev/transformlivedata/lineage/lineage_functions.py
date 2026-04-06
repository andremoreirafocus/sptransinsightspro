from typing import Dict, Any


def get_json_raw_fields_path_from_schema(schema: Dict[str, Any]) -> Dict[str, str]:
    payload_props = (
        schema.get("properties", {}).get("payload", {}).get("properties", {})
    )
    first_level_list = None
    for key, value in payload_props.items():
        if value.get("type") == "array":
            first_level_list = key
            break
    if not first_level_list:
        return {}
    first_level_items = payload_props.get(first_level_list, {}).get("items", {})
    first_level_props = first_level_items.get("properties", {})
    second_level_list = None
    for key, value in first_level_props.items():
        if value.get("type") == "array":
            second_level_list = key
            break
    if not second_level_list:
        return {}
    second_level_items = first_level_props.get(second_level_list, {}).get("items", {})
    ref = second_level_items.get("$ref", "")
    ref_name = ref.split("/")[-1] if ref else None
    if not ref_name:
        return {}
    vehicle_props = (
        schema.get("definitions", {}).get(ref_name, {}).get("properties", {})
    )
    raw_fields_path_map = {}
    for key in first_level_props.keys():
        if key == second_level_list:
            continue
        raw_fields_path_map[key] = f"payload.{first_level_list}[i].{key}"
    for key in vehicle_props.keys():
        raw_fields_path_map[key] = (
            f"payload.{first_level_list}[i].{second_level_list}[j].{key}"
        )
    return raw_fields_path_map


def build_api_lineage(
    df: Any, rename_map: Dict[str, str], raw_fields_path_map: Dict[str, str]
) -> Dict[str, Any]:
    lineage = {}
    for raw_key, out_col in rename_map.items():
        lineage[out_col] = {
            "inputs": [raw_fields_path_map.get(raw_key, raw_key)],
            "type": get_column_type(df, out_col),
            "transformation": "API rename/cast",
        }
    return lineage


def build_join_lineage(
    df: Any,
    merge_table_name: str,
    merge_key: str,
    trip_details_columns: Any,
) -> Dict[str, Any]:
    lineage = {}
    for col in trip_details_columns:
        if col == merge_key or col not in df.columns:
            continue
        lineage[col] = {
            "inputs": [f"{merge_table_name}.{col}"],
            "type": get_column_type(df, col),
            "transformation": f"{merge_table_name} left join",
        }
    return lineage


def merge_lineage_fragments(*fragments: Dict[str, Any]) -> Dict[str, Any]:
    lineage = {}
    for fragment in fragments:
        lineage.update(fragment)
    return lineage


def get_column_type(df: Any, column: str) -> str:
    if column not in df.columns:
        return "unknown"
    dtype = str(df[column].dtype)
    if dtype.startswith("int"):
        return "int"
    if dtype.startswith("float"):
        return "float"
    if dtype.startswith("bool"):
        return "boolean"
    if "datetime" in dtype:
        return "timestamp"
    return "string"
