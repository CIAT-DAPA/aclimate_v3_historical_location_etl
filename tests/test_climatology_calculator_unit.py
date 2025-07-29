from aclimate_v3_historical_location_etl.climate_processing import (
    ClimatologyCalculator,
)


def test_calculate_monthly_climatology_returns_list():
    calc = ClimatologyCalculator()
    # Simulate historical data for 2 months, 2 measures
    data = [
        {"date": "2020-01-15", "measure_id": 1, "value": 10},
        {"date": "2020-01-20", "measure_id": 1, "value": 20},
        {"date": "2020-02-15", "measure_id": 2, "value": 30},
        {"date": "2020-02-20", "measure_id": 2, "value": 40},
    ]
    result = calc.calculate_monthly_climatology(data, 1)
    assert isinstance(result, list)
    assert all(hasattr(r, "location_id") for r in result)


def test_group_by_month_handles_empty():
    calc = ClimatologyCalculator()
    result = calc._group_by_month([])
    assert isinstance(result, dict)
    assert len(result) == 0
