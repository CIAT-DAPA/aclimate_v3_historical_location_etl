from datetime import datetime

import pandas as pd

from aclimate_v3_historical_location_etl.data_managment import DataValidator


def test_validate_extracted_data_passes():
    validator = DataValidator()
    df = pd.DataFrame(
        {
            "location_id": [1, 1],
            "location_name": ["A", "A"],
            "latitude": [10, 10],
            "longitude": [20, 20],
            "date": [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            "prec": [1, 2],
        }
    )
    cleaned, results = validator.validate_extracted_data(
        df, datetime(2020, 1, 1), datetime(2020, 1, 2), [1]
    )
    assert isinstance(cleaned, pd.DataFrame)
    assert "is_valid" in results


def test_validate_extracted_data_handles_empty():
    validator = DataValidator()
    df = pd.DataFrame()
    cleaned, results = validator.validate_extracted_data(
        df, datetime(2020, 1, 1), datetime(2020, 1, 2), [1]
    )
    assert not results["is_valid"]


def test_generate_validation_report_runs():
    validator = DataValidator()
    results = {
        "is_valid": True,
        "errors": [],
        "warnings": [],
        "statistics": {
            "total_records": 2,
            "total_columns": 5,
            "has_valid_structure": True,
        },
        "cleaning_actions": ["none"],
    }
    report = validator.generate_validation_report(results)
    assert isinstance(report, str)
