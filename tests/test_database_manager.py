import unittest
from unittest.mock import MagicMock

from aclimate_v3_historical_location_etl.data_managment import DatabaseManager


class TestDatabaseManager(unittest.TestCase):
    def setUp(self):
        self.db_manager = DatabaseManager()
        self.db_manager.historical_monthly_service = MagicMock()

    def test_get_historical_monthly_by_location_id_returns_list(self):
        fake_result = [MagicMock(), MagicMock()]
        self.db_manager.historical_monthly_service.get_by_location_id.return_value = (
            fake_result
        )
        result = self.db_manager.get_historical_monthly_by_location_id(1)
        self.assertEqual(result, fake_result)
        self.db_manager.historical_monthly_service.get_by_location_id.assert_called_once_with(
            1
        )

    def test_get_historical_monthly_by_location_id_handles_exception(self):
        self.db_manager.historical_monthly_service.get_by_location_id.side_effect = (
            Exception("fail")
        )
        with self.assertLogs(level="ERROR"):
            result = self.db_manager.get_historical_monthly_by_location_id(1)
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
