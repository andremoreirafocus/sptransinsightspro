from typing import Dict, Tuple, List, Union

import jsonschema  # type: ignore[import-untyped]
import json


def validate_json_data_schema(
    data: Dict, schema: Union[Dict, str]
) -> Tuple[bool, List[str]]:
    try:
        schema_dict = load_raw_schema(schema) if isinstance(schema, str) else schema
        jsonschema.validate(instance=data, schema=schema_dict)
        return True, []
    except jsonschema.ValidationError as e:
        error_msg = str(e)
        return False, [error_msg]
    except Exception:
        raise


def load_raw_schema(config_file: str) -> Dict:
    with open(config_file) as f:
        schema = json.load(f)
    return schema
