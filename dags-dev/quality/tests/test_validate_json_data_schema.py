import pytest

from quality.validate_json_data_schema import validate_json_data_schema

VALID_SCHEMA = {
    "type": "object",
    "properties": {"name": {"type": "string"}},
    "required": ["name"],
}


def test_returns_true_on_valid_data():
    is_valid, errors = validate_json_data_schema({"name": "bus"}, VALID_SCHEMA)
    assert is_valid is True
    assert errors == []


def test_returns_false_on_validation_error():
    is_valid, errors = validate_json_data_schema({"name": 123}, VALID_SCHEMA)
    assert is_valid is False
    assert len(errors) == 1


def test_raises_on_unexpected_exception():
    with pytest.raises(Exception):
        validate_json_data_schema({"name": "bus"}, "/nonexistent/path/schema.json")
