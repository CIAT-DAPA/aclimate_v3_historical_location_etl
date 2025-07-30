from unittest.mock import MagicMock

from aclimate_v3_historical_location_etl.data_managment import GeoServerClient


def test_init_sets_config_and_validator():
    config = {"a": 1}
    client = GeoServerClient(config)
    assert client.geoserver_config == config
    assert hasattr(client, "data_validator")


def test_extract_location_data_returns_dataframe(monkeypatch):
    import pandas as pd

    config = {"a": 1}
    client = GeoServerClient(config)
    # Mock db_manager and data_validator
    client.db_manager = MagicMock()
    # Siempre devolver al menos una ubicación válida
    MockLoc = type("Loc", (), {"id": 1})
    client.db_manager.get_all_locations = MagicMock(return_value=[MockLoc()])
    client.db_manager.get_locations_by_ids = MagicMock(return_value=[MockLoc()])
    # Mock _extract_location_point_data para devolver datos válidos
    client._extract_location_point_data = MagicMock(
        return_value=({"location_id": 1, "date": "2020-01-01"}, {"success": True})
    )
    # No monkeypatch de DataFrame, usamos el real
    # Patch data_validator
    client.data_validator.validate_extracted_data = MagicMock(
        return_value=(
            pd.DataFrame({"location_id": [1], "date": ["2020-01-01"]}),
            {
                "is_valid": True,
                "errors": [],
                "warnings": [],
                "statistics": {},
                "cleaning_actions": [],
            },
        )
    )
    client.data_validator.generate_validation_report = MagicMock(return_value="OK")
    # Patch info y sys.exit para evitar que termine el test
    import src.aclimate_v3_historical_location_etl.tools.logging_manager as lm

    lm.info = MagicMock()
    import sys

    monkeypatch.setattr(
        sys, "exit", lambda code=0: (_ for _ in ()).throw(Exception("sys.exit called"))
    )
    # Call with 'all' locations
    result = client.extract_location_data(
        "all", "HONDURAS", pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-01")
    )
    assert hasattr(result, "columns")


def test_extract_point_from_tiff_returns_none_on_exception():
    client = GeoServerClient({"a": 1})
    # Should return None if exception
    assert client._extract_point_from_tiff(b"bad", 0, 0) is None


def test_interpolate_spatial_value_returns_none_on_exception():
    client = GeoServerClient({"a": 1})
    # Should return None if exception
    assert client._interpolate_spatial_value(None, 0, 0) is None


def test_inverse_distance_weighting_returns_float_on_exception():
    client = GeoServerClient({"a": 1})
    # Should return float (default) if exception
    result = client._inverse_distance_weighting(0, 0, [], [], 2.0)
    assert isinstance(result, float)
