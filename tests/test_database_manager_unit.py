from unittest.mock import MagicMock, patch

import pytest

from aclimate_v3_historical_location_etl.data_managment import DatabaseManager


@pytest.fixture
def db_manager():
    return DatabaseManager()


def test_get_locations_by_ids_returns_locations(db_manager):
    MockLoc = type("MockLoc", (), {"name": "loc", "id": 1})
    db_manager.location_service.get_by_id = MagicMock(return_value=MockLoc())
    result = db_manager.get_locations_by_ids("1,2", "HONDURAS")
    assert isinstance(result, list)
    assert all(hasattr(x, "name") for x in result)


def test_get_historical_monthly_by_location_id_returns_list(db_manager):
    db_manager.historical_monthly_service.get_by_location_id = MagicMock(
        return_value=[{"id": 1}]
    )
    result = db_manager.get_historical_monthly_by_location_id(1)
    assert isinstance(result, list)


def test_save_or_update_climatology_calls_service(db_manager):
    db_manager.climatology_service.get_by_location_id = MagicMock(return_value=[])
    db_manager.climatology_service.create = MagicMock()
    db_manager.climatology_service.update = MagicMock()
    MockClim = type(
        "MockClim", (), {"location_id": 1, "measure_id": 1, "month": 1, "value": 1.0}
    )
    db_manager.save_or_update_climatology(1, [MockClim()])
    assert (
        db_manager.climatology_service.create.called
        or db_manager.climatology_service.update.called
    )


def test_get_all_locations_returns_list(db_manager):
    db_manager.location_service.get_all = MagicMock(return_value=["loc"])
    result = db_manager.get_all_locations("HONDURAS")
    assert isinstance(result, list)


def test_validate_location_exists_true(db_manager):
    db_manager.location_service.get_by_id = MagicMock(return_value=True)
    assert db_manager.validate_location_exists(1)


def test_get_location_info_returns_location(db_manager):
    MockLoc = type("MockLoc", (), {"name": "loc", "id": 1})
    db_manager.location_service.get_by_id = MagicMock(return_value=MockLoc())
    result = db_manager.get_location_info(1)
    assert result is not None
    assert hasattr(result, "name")


def test_get_geoserver_config_returns_dict(db_manager):
    # Mock country_service.get_by_name y data_source_service.get_by_country
    MockCountry = type("MockCountry", (), {"id": 1})
    db_manager.country_service.get_by_name = MagicMock(return_value=[MockCountry()])
    MockConfig = type("MockConfig", (), {"name": "name", "content": "{}"})
    db_manager.data_source_service.get_by_country = MagicMock(
        return_value=[MockConfig()]
    )
    with patch("json.loads", return_value={"a": 1}):
        result = db_manager.get_geoserver_config("name", "HONDURAS")
        assert isinstance(result, dict)
        assert result == {"a": 1}


def test_get_measure_id_by_short_name_returns_id(db_manager):
    MockMeasure = type("MockMeasure", (), {"id": 5})
    db_manager.climate_measure_service.get_by_short_name = MagicMock(
        return_value=[MockMeasure()]
    )
    assert db_manager.get_measure_id_by_short_name("tmax") == 5


def test_get_climate_measure_mapping_returns_dict(db_manager):
    db_manager.climate_measure_service.get_all = MagicMock(
        return_value=[type("obj", (), {"short_name": "tmax", "id": 1})()]
    )
    result = db_manager.get_climate_measure_mapping()
    assert isinstance(result, dict)
