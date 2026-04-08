from transformlivedata.services.transform_positions import (
    get_trip_id,
    flatten_raw_positions,
    normalize_columns,
    add_trip_id,
    enrich_with_trip_details,
    calculate_distance,
    compute_distances,
    build_metrics_and_issues,
    build_transformation_result,
    transform_positions,
)
from transformlivedata.services import transform_positions as tp
import pytest
import pandas as pd


def test_get_trip_id():
    assert get_trip_id("12345", 1) == "12345-0", (
        "Trip ID should be in the format 'vehicle_id-sequence_number'"
    )
    assert get_trip_id("12345", 2) == "12345-1", (
        "Trip ID should be in the format 'vehicle_id-sequence_number'"
    )


def test_calculate_distance():
    nyc_lat, nyc_long = (40.7128, -74.0060)
    la_lat, la_long = (34.0522, -118.2437)
    distance = calculate_distance(nyc_lat, nyc_long, la_lat, la_long)
    assert distance == (round(3935746), True), (
        "Distance calculation should be approximately 3935746 meters"
    )


def test_flatten_positions():
    raw_positions = {
        "metadata": {
            "extracted_at": "2026-02-15T09:36:12.651772",
            "source": "sptrans_api_v2",
            "total_vehicles": 4,
        },
        "payload": {
            "hr": "09:36",
            "l": [
                {
                    "c": "2104-10",
                    "cl": 35023,
                    "sl": 2,
                    "lt0": "TERM. PQ. D. PEDRO II",
                    "lt1": "METRÔ SANTANA",
                    "qv": 2,
                    "vs": [
                        {
                            "p": 21300,
                            "a": True,
                            "ta": "2026-02-15T12:35:41Z",
                            "py": -23.509144,
                            "px": -46.624826999999996,
                            "sv": None,
                            "is": None,
                        },
                        {
                            "p": 21303,
                            "a": True,
                            "ta": "2026-02-15T12:35:58Z",
                            "py": -23.502574,
                            "px": -46.624022999999994,
                            "sv": None,
                            "is": None,
                        },
                    ],
                },
                {
                    "c": "3063-1",
                    "cl": 35394,
                    "sl": 2,
                    "lt0": "TERM. SÃO MATEUS",
                    "lt1": "GUAIANASES",
                    "qv": 2,
                    "vs": [
                        {
                            "p": 48799,
                            "a": True,
                            "ta": "2026-02-15T12:35:34Z",
                            "py": -23.558149,
                            "px": -46.399587499999996,
                            "sv": None,
                            "is": None,
                        },
                        {
                            "p": 48280,
                            "a": True,
                            "ta": "2026-02-15T12:35:45Z",
                            "py": -23.5591695,
                            "px": -46.3996545,
                            "sv": None,
                            "is": None,
                        },
                    ],
                },
            ],
        },
    }

    df = pd.DataFrame(raw_positions)

    # Flatten the positions
    df_flat = flatten_raw_positions(df)
    print(df_flat.head())  # Debug: Check the structure of the flattened DataFrame
    print(df_flat.columns)  # Debug: Check the structure of the flattened DataFrame

    # Check if the flattened DataFrame has the expected structure
    expected_columns = [
        "p",
        "a",
        "ta",
        "py",
        "px",
        "sv",
        "is",
        "c",
        "cl",
        "sl",
        "lt0",
        "lt1",
        "qv",
    ]
    for col in expected_columns:
        assert col in df_flat.columns, (
            f"Column '{col}' is missing in the flattened DataFrame"
        )

    expected_rows = sum(len(line["vs"]) for line in raw_positions["payload"]["l"])
    assert len(df_flat) == expected_rows, (
        f"Expected {expected_rows} rows in the flattened DataFrame, but got {len(df_flat)}"
    )

    expected_rows_by_vehicle = {
        21300: {
            "c": "2104-10",
            "cl": 35023,
            "ta": "2026-02-15T12:35:41Z",
            "py": -23.509144,
            "px": -46.624826999999996,
        },
        48280: {
            "c": "3063-1",
            "cl": 35394,
            "ta": "2026-02-15T12:35:45Z",
            "py": -23.5591695,
            "px": -46.3996545,
        },
    }
    for vehicle_id, expected in expected_rows_by_vehicle.items():
        row = df_flat.loc[df_flat["p"] == vehicle_id].iloc[0]
        row_dict = row[list(expected.keys())].to_dict()
        assert row_dict == expected, (
            f"Row {row_dict} for vehicle {vehicle_id} does not match expected values: {expected}"
        )


def test_normalize_columns():
    df_flat = pd.DataFrame(
        [
            {
                "c": "2104-10",
                "cl": 35023,
                "sl": 2,
                "lt0": "TERM. PQ. D. PEDRO II",
                "lt1": "METRÔ SANTANA",
                "qv": 2,
                "p": 21300,
                "a": True,
                "ta": "2026-02-15T12:35:41Z",
                "py": -23.509144,
                "px": -46.624826999999996,
            }
        ]
    )
    rename_map = {
        "c": "linha_lt",
        "cl": "linha_code",
        "sl": "linha_sentido",
        "lt0": "lt_destino",
        "lt1": "lt_origem",
        "p": "veiculo_id",
        "a": "veiculo_acessivel",
        "ta": "veiculo_ts",
        "py": "veiculo_lat",
        "px": "veiculo_long",
    }
    raw_path_map = {
        "c": "payload.l[i].c",
        "cl": "payload.l[i].cl",
        "sl": "payload.l[i].sl",
        "lt0": "payload.l[i].lt0",
        "lt1": "payload.l[i].lt1",
        "p": "payload.l[i].vs[j].p",
        "a": "payload.l[i].vs[j].a",
        "ta": "payload.l[i].vs[j].ta",
        "py": "payload.l[i].vs[j].py",
        "px": "payload.l[i].vs[j].px",
    }
    metadata = {"extracted_at": "2026-02-15T09:36:12.651772"}

    df_norm, lineage = normalize_columns(df_flat, rename_map, raw_path_map, metadata)

    expected_columns = [
        "linha_lt",
        "linha_code",
        "linha_sentido",
        "lt_destino",
        "lt_origem",
        "qv",
        "veiculo_id",
        "veiculo_acessivel",
        "veiculo_ts",
        "veiculo_lat",
        "veiculo_long",
        "extracao_ts",
    ]
    for col in expected_columns:
        assert col in df_norm.columns, (
            f"Column '{col}' is missing in normalized DataFrame"
        )

    assert str(df_norm["linha_sentido"].dtype) == "Int64"
    assert str(df_norm["linha_code"].dtype) == "Int64"
    assert str(df_norm["veiculo_id"].dtype) == "Int64"
    assert pd.api.types.is_datetime64_any_dtype(df_norm["veiculo_ts"])
    assert pd.api.types.is_datetime64_any_dtype(df_norm["extracao_ts"])

    assert lineage["linha_lt"]["inputs"] == ["payload.l[i].c"]
    assert lineage["veiculo_id"]["inputs"] == ["payload.l[i].vs[j].p"]
    assert "extracao_ts" in lineage


def test_add_trip_id():
    df = pd.DataFrame(
        [
            {"linha_lt": "2104-10", "linha_sentido": 1},
            {"linha_lt": "2104-10", "linha_sentido": 2},
        ]
    )
    df_with_trip = add_trip_id(df)
    expected_trip_ids = ["2104-10-0", "2104-10-1"]
    extracted_trip_ids = df_with_trip["trip_id"].tolist()
    assert extracted_trip_ids == expected_trip_ids, (
        f"Trip IDs {extracted_trip_ids} should be {expected_trip_ids}"
    )


def test_enrich_with_trip_details():
    df_positions = pd.DataFrame(
        [
            {
                "trip_id": "2104-10-0",
                "veiculo_id": 21300,
                "linha_lt": "2104-10",
                "linha_code": 35023,
                "linha_sentido": 1,
                "lt_destino": "TERM. PQ. D. PEDRO II",
                "lt_origem": "METRÔ SANTANA",
                "veiculo_acessivel": True,
                "veiculo_ts": "2026-02-15T12:35:41Z",
                "veiculo_lat": -23.509144,
                "veiculo_long": -46.624826999999996,
            },
            {
                "trip_id": "2104-10-1",
                "veiculo_id": 21301,
                "linha_lt": "2104-10",
                "linha_code": 35023,
                "linha_sentido": 2,
                "lt_destino": "TERM. PQ. D. PEDRO II",
                "lt_origem": "METRÔ SANTANA",
                "veiculo_acessivel": True,
                "veiculo_ts": "2026-02-15T12:35:58Z",
                "veiculo_lat": -23.502574,
                "veiculo_long": -46.624022999999994,
            },
        ]
    )
    trip_details_df = pd.DataFrame(
        [
            {
                "trip_id": "2104-10-0",
                "is_circular": False,
                "first_stop_id": 123,
                "first_stop_lat": -23.500001,
                "first_stop_lon": -46.600001,
                "last_stop_id": 456,
                "last_stop_lat": -23.599999,
                "last_stop_lon": -46.699999,
            }
        ]
    )
    df_enriched, lineage = enrich_with_trip_details(df_positions, trip_details_df)
    row_match = df_enriched.loc[df_enriched["trip_id"] == "2104-10-0"].iloc[0]
    expected_match = {
        "is_circular": False,
        "first_stop_id": 123,
        "first_stop_lat": -23.500001,
        "first_stop_lon": -46.600001,
        "last_stop_id": 456,
        "last_stop_lat": -23.599999,
        "last_stop_lon": -46.699999,
        "_merge": "both",
    }
    row_match_dict = row_match[list(expected_match.keys())].to_dict()
    assert row_match_dict == expected_match, (
        f"Row {row_match_dict} does not match expected values: {expected_match}"
    )
    row_no_match = df_enriched.loc[df_enriched["trip_id"] == "2104-10-1"].iloc[0]
    assert row_no_match["_merge"] == "left_only"
    expected_lineage_cols = [
        "is_circular",
        "first_stop_id",
        "first_stop_lat",
        "first_stop_lon",
        "last_stop_id",
        "last_stop_lat",
        "last_stop_lon",
    ]
    for col in expected_lineage_cols:
        assert col in lineage
        assert lineage[col]["inputs"] == [f"trip_details.{col}"]
        assert lineage[col]["transformation"] == "trip_details left join"
        assert lineage[col]["type"] not in (None, "")


def test_compute_distances():
    df = pd.DataFrame(
        [
            {
                "veiculo_id": 21300,
                "linha_lt": "2104-10",
                "veiculo_lat": -23.509144,
                "veiculo_long": -46.624826999999996,
                "first_stop_lat": -23.510000,
                "first_stop_lon": -46.625000,
                "last_stop_lat": -23.520000,
                "last_stop_lon": -46.630000,
            }
        ]
    )
    df_out, distance_errors, lineage = compute_distances(df)
    for col in ["distance_to_first_stop", "distance_to_last_stop"]:
        assert col in df_out.columns, f"Column '{col}' is missing in output DataFrame"
    assert distance_errors == []
    assert df_out["distance_to_first_stop"].iloc[0] > 0, (
        "Distance to first stop should be greater than 0"
    )
    assert df_out["distance_to_last_stop"].iloc[0] > 0, (
        "Distance to last stop should be greater than 0"
    )
    assert lineage["distance_to_first_stop"]["inputs"] == [
        "veiculo_lat",
        "veiculo_long",
        "first_stop_lat",
        "first_stop_lon",
    ], "Lineage for distance_to_first_stop should include correct input columns"
    assert lineage["distance_to_last_stop"]["inputs"] == [
        "veiculo_lat",
        "veiculo_long",
        "last_stop_lat",
        "last_stop_lon",
    ], "Lineage for distance_to_last_stop should include correct input columns"
    assert (
        lineage["distance_to_first_stop"]["transformation"]
        == "calculated based on current position"
    ), (
        "Lineage for distance_to_first_stop should indicate calculated based on current position transformation"
    )
    assert (
        lineage["distance_to_last_stop"]["transformation"]
        == "calculated based on current position"
    ), (
        "Lineage for distance_to_last_stop should indicate calculated based on current position transformation"
    )
    assert lineage["distance_to_first_stop"]["type"] == "float", (
        "Lineage for distance_to_first_stop should indicate metric type"
    )
    assert lineage["distance_to_last_stop"]["type"] == "float", (
        "Lineage for distance_to_last_stop should indicate metric type"
    )


def test_build_metrics_and_issues():
    raw_positions = {
        "metadata": {"total_vehicles": 3},
        "payload": {"l": [{}, {}]},
    }
    valid_df = pd.DataFrame(
        [
            {"veiculo_id": 1, "trip_id": "T-1", "linha_lt": "1000-10"},
        ]
    )
    invalid_df = pd.DataFrame(
        [
            {"veiculo_id": 2, "trip_id": "T-2", "linha_lt": "2000-10"},
            {"veiculo_id": 3, "trip_id": "T-3", "linha_lt": "2000-10"},
        ]
    )
    distance_errors = [{"vehicle_id": 2, "linha": "2000-10", "error_type": "x"}]
    metrics, issues = build_metrics_and_issues(
        raw_positions, valid_df, invalid_df, distance_errors
    )
    assert metrics == {
        "total_vehicles_processed": 3,
        "valid_vehicles": 1,
        "invalid_vehicles": 2,
        "expected_vehicles": 3,
        "total_lines_processed": 2,
    }, "Metrics should match expected values"
    assert issues["invalid_vehicle_ids"] == [2, 3], (
        "Invalid vehicle IDs list should be 2 and 3"
    )
    assert set(issues["invalid_trips"]) == {"T-2", "T-3"}, (
        "Invalid trips list should be T-2 and T-3"
    )
    assert issues["distance_calculation_errors"] == distance_errors, (
        "Distance calculation errors should match"
    )
    assert issues["lines_with_invalid_vehicles"] == 1, (
        "Lines with invalid vehicles should be 1"
    )


def test_build_transformation_result():
    valid_df = pd.DataFrame(
        [
            {
                "extracao_ts": "2026-02-15T09:36:12.651772",
                "veiculo_id": 1,
                "linha_lt": "1000-10",
                "distance_to_first_stop": 10.0,
                "distance_to_last_stop": 20.0,
            }
        ]
    )
    invalid_df = pd.DataFrame(
        [
            {
                "extracao_ts": "2026-02-15T09:36:12.651772",
                "veiculo_id": 2,
                "linha_lt": "2000-10",
                "distance_to_first_stop": None,
                "distance_to_last_stop": None,
                "invalid_reason": "transform_error",
                "validation_failed_at": "2026-02-15T10:00:00Z",
            }
        ]
    )
    valid_df_columns = [
        "extracao_ts",
        "veiculo_id",
        "linha_lt",
        "distance_to_first_stop",
        "distance_to_last_stop",
    ]
    metrics = {
        "total_vehicles_processed": 2,
        "valid_vehicles": 1,
        "invalid_vehicles": 1,
        "expected_vehicles": 2,
        "total_lines_processed": 1,
    }
    issues = {
        "invalid_vehicle_ids": [2],
        "invalid_trips": [],
        "distance_calculation_errors": [],
        "lines_with_invalid_vehicles": 1,
    }
    lineage = {
        "linha_lt": {
            "inputs": ["payload.l[i].c"],
            "type": "string",
            "transformation": "API rename/cast",
        }
    }
    batch_ts = "2026-02-15T09:36:12.651772"

    result = build_transformation_result(
        valid_df,
        invalid_df,
        valid_df_columns,
        metrics,
        issues,
        batch_ts,
        lineage,
    )

    assert list(result["positions"].columns) == valid_df_columns, (
        "Positions columns should match valid_df_columns"
    )
    assert list(result["invalid_positions"].columns) == valid_df_columns + [
        "invalid_reason",
        "validation_failed_at",
    ], "Invalid positions should include reason and validation timestamp columns"
    assert result["metrics"] == metrics
    assert result["issues"] == issues
    assert result["batch_ts"] == batch_ts
    assert result["lineage"] == lineage
    assert result["quality_score"] == 50.0, "Quality score should be 50.0"


@pytest.fixture
def raw_positions_valid():
    return {
        "metadata": {"extracted_at": "2026-02-15T09:36:12.651772", "total_vehicles": 2},
        "payload": {
            "hr": "09:36",
            "l": [
                {
                    "c": "2104-10",
                    "cl": 35023,
                    "sl": 1,
                    "lt0": "TERM. PQ. D. PEDRO II",
                    "lt1": "METRÔ SANTANA",
                    "qv": 1,
                    "vs": [
                        {
                            "p": 21300,
                            "a": True,
                            "ta": "2026-02-15T12:35:41Z",
                            "py": -23.509144,
                            "px": -46.624826999999996,
                            "sv": None,
                            "is": None,
                        }
                    ],
                },
                {
                    "c": "2104-10",
                    "cl": 35023,
                    "sl": 2,
                    "lt0": "TERM. PQ. D. PEDRO II",
                    "lt1": "METRÔ SANTANA",
                    "qv": 1,
                    "vs": [
                        {
                            "p": 21301,
                            "a": True,
                            "ta": "2026-02-15T12:35:58Z",
                            "py": -23.502574,
                            "px": -46.624022999999994,
                            "sv": None,
                            "is": None,
                        }
                    ],
                },
            ],
        },
    }


@pytest.fixture
def trip_details_valid():
    return pd.DataFrame(
        [
            {
                "trip_id": "2104-10-0",
                "is_circular": False,
                "first_stop_id": 123,
                "first_stop_lat": -23.510000,
                "first_stop_lon": -46.625000,
                "last_stop_id": 456,
                "last_stop_lat": -23.520000,
                "last_stop_lon": -46.630000,
            },
            {
                "trip_id": "2104-10-1",
                "is_circular": False,
                "first_stop_id": 123,
                "first_stop_lat": -23.510000,
                "first_stop_lon": -46.625000,
                "last_stop_id": 456,
                "last_stop_lat": -23.520000,
                "last_stop_lon": -46.630000,
            },
        ]
    )


@pytest.fixture
def config_valid():
    return {"general": {}, "raw_data_json_schema": {"dummy": True}}


@pytest.fixture(autouse=True)
def raw_path_map_stub(monkeypatch):
    monkeypatch.setattr(
        tp,
        "get_json_raw_fields_path_from_schema",
        lambda _: {
            "c": "payload.l[i].c",
            "cl": "payload.l[i].cl",
            "sl": "payload.l[i].sl",
            "lt0": "payload.l[i].lt0",
            "lt1": "payload.l[i].lt1",
            "p": "payload.l[i].vs[j].p",
            "a": "payload.l[i].vs[j].a",
            "ta": "payload.l[i].vs[j].ta",
            "py": "payload.l[i].vs[j].py",
            "px": "payload.l[i].vs[j].px",
        },
    )


def test_transform_positions_success(
    monkeypatch, raw_positions_valid, trip_details_valid, config_valid
):
    monkeypatch.setattr(tp, "load_trip_details", lambda _: trip_details_valid)
    result = transform_positions(config_valid, raw_positions_valid)

    assert result["invalid_positions"].empty
    assert result["positions"].shape[0] == 2
    assert set(result["positions"]["veiculo_id"].tolist()) == {21300, 21301}
    assert result["metrics"]["valid_vehicles"] == 2
    assert result["metrics"]["invalid_vehicles"] == 0
    assert result["quality_score"] == 100.0


def test_transform_positions_missing_schema(raw_positions_valid):
    with pytest.raises(ValueError):
        transform_positions({"general": {}}, raw_positions_valid)


def test_transform_positions_trip_details_empty(monkeypatch, raw_positions_valid, config_valid):
    monkeypatch.setattr(tp, "load_trip_details", lambda _: pd.DataFrame())
    with pytest.raises(ValueError):
        transform_positions(config_valid, raw_positions_valid)


def test_transform_positions_flatten_empty(monkeypatch, raw_positions_valid, trip_details_valid, config_valid):
    monkeypatch.setattr(tp, "load_trip_details", lambda _: trip_details_valid)
    monkeypatch.setattr(tp, "flatten_raw_positions", lambda _: pd.DataFrame())
    with pytest.raises(ValueError):
        transform_positions(config_valid, raw_positions_valid)


def test_transform_positions_normalize_empty(monkeypatch, raw_positions_valid, trip_details_valid, config_valid):
    monkeypatch.setattr(tp, "load_trip_details", lambda _: trip_details_valid)
    monkeypatch.setattr(tp, "normalize_columns", lambda *args, **kwargs: (pd.DataFrame(), {}))
    with pytest.raises(ValueError):
        transform_positions(config_valid, raw_positions_valid)


def test_transform_positions_enrich_empty(monkeypatch, raw_positions_valid, trip_details_valid, config_valid):
    monkeypatch.setattr(tp, "load_trip_details", lambda _: trip_details_valid)
    monkeypatch.setattr(tp, "enrich_with_trip_details", lambda *args, **kwargs: (pd.DataFrame(), {}))
    with pytest.raises(ValueError):
        transform_positions(config_valid, raw_positions_valid)


def test_transform_positions_no_valid_after_enrich(
    monkeypatch, raw_positions_valid, trip_details_valid, config_valid
):
    monkeypatch.setattr(tp, "load_trip_details", lambda _: trip_details_valid)
    def enrich_left_only(df, trip_df):
        df = df.copy()
        df["_merge"] = "left_only"
        return df, {}
    monkeypatch.setattr(tp, "enrich_with_trip_details", enrich_left_only)
    with pytest.raises(ValueError):
        transform_positions(config_valid, raw_positions_valid)


def test_transform_positions_distance_calc_empty(
    monkeypatch, raw_positions_valid, trip_details_valid, config_valid
):
    monkeypatch.setattr(tp, "load_trip_details", lambda _: trip_details_valid)
    monkeypatch.setattr(tp, "compute_distances", lambda *args, **kwargs: (pd.DataFrame(), [], {}))
    with pytest.raises(ValueError):
        transform_positions(config_valid, raw_positions_valid)


def test_transform_positions_distance_errors(
    monkeypatch, raw_positions_valid, trip_details_valid, config_valid
):
    monkeypatch.setattr(tp, "load_trip_details", lambda _: trip_details_valid)

    def calc_distance(lat1, lon1, lat2, lon2):
        if lat1 == 0.0:
            return -1.0, False
        return 100.0, True

    monkeypatch.setattr(tp, "calculate_distance", calc_distance)

    raw_positions = raw_positions_valid.copy()
    raw_positions["payload"]["l"][0]["vs"][0]["py"] = 0.0
    result = transform_positions(config_valid, raw_positions)

    assert set(result["positions"]["veiculo_id"].tolist()) == {21301}
    assert set(result["invalid_positions"]["veiculo_id"].tolist()) == {21300}
    assert result["invalid_positions"]["invalid_reason"].iloc[0] == (
        "transform_error:distance_calculation"
    )


def test_transform_positions_missing_trips(
    monkeypatch, raw_positions_valid, trip_details_valid, config_valid
):
    trip_details_partial = trip_details_valid[trip_details_valid["trip_id"] == "2104-10-0"]
    monkeypatch.setattr(tp, "load_trip_details", lambda _: trip_details_partial)
    result = transform_positions(config_valid, raw_positions_valid)

    assert set(result["positions"]["veiculo_id"].tolist()) == {21300}
    assert set(result["invalid_positions"]["veiculo_id"].tolist()) == {21301}
    assert result["invalid_positions"]["invalid_reason"].iloc[0] == (
        "transform_error:trip_details_missing"
    )
