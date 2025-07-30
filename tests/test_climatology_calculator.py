import unittest

from aclimate_v3_historical_location_etl.climate_processing import (
    ClimatologyCalculator,
)


class TestClimatologyCalculator(unittest.TestCase):
    def setUp(self):
        self.calculator = ClimatologyCalculator()

    def test_group_by_month_empty(self):
        result = self.calculator._group_by_month([])
        self.assertEqual(result, {})

    def test_group_by_month_basic(self):
        data = [
            {"date": "2020-01-01", "measure_id": 1, "value": 10.0},
            {"date": "2020-01-15", "measure_id": 1, "value": 12.0},
            {"date": "2020-02-01", "measure_id": 1, "value": 20.0},
        ]
        result = self.calculator._group_by_month(data)
        self.assertIn(1, result)
        self.assertIn(2, result)
        self.assertEqual(len(result[1]), 2)
        self.assertEqual(len(result[2]), 1)

    def test_calculate_monthly_climatology(self):
        data = [
            {"date": "2020-01-01", "measure_id": 1, "value": 10.0},
            {"date": "2020-01-15", "measure_id": 1, "value": 14.0},
            {"date": "2020-02-01", "measure_id": 1, "value": 20.0},
            {"date": "2020-01-01", "measure_id": 2, "value": 5.0},
        ]
        result = self.calculator.calculate_monthly_climatology(data, 99)
        self.assertTrue(
            any(
                r.measure_id == 1 and r.month == 1 and abs(r.value - 12.0) < 1e-6
                for r in result
            )
        )
        self.assertTrue(
            any(
                r.measure_id == 1 and r.month == 2 and abs(r.value - 20.0) < 1e-6
                for r in result
            )
        )
        self.assertTrue(
            any(
                r.measure_id == 2 and r.month == 1 and abs(r.value - 5.0) < 1e-6
                for r in result
            )
        )


if __name__ == "__main__":
    unittest.main()
