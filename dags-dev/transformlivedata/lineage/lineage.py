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
    second_level_items = (
        first_level_props.get(second_level_list, {}).get("items", {})
    )
    ref = second_level_items.get("$ref", "")
    ref_name = ref.split("/")[-1] if ref else None
    if not ref_name:
        return {}
    vehicle_props = schema.get("definitions", {}).get(ref_name, {}).get("properties", {})
    raw_path_map = {}
    for key in first_level_props.keys():
        if key == second_level_list:
            continue
        raw_path_map[key] = f"payload.{first_level_list}[i].{key}"
    for key in vehicle_props.keys():
        raw_path_map[key] = (
            f"payload.{first_level_list}[i].{second_level_list}[j].{key}"
        )
    return raw_path_map
