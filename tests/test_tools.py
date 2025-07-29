import unittest
from unittest.mock import MagicMock

from aclimate_v3_historical_location_etl.tools.tools import DownloadProgressBar


class TestDownloadProgressBar(unittest.TestCase):
    def test_update_to_sets_total_and_updates(self):
        bar = DownloadProgressBar()
        bar.update = MagicMock()
        bar.n = 0
        bar.total = None
        bar.update_to(2, 10, 100)
        self.assertEqual(bar.total, 100)
        bar.update.assert_called_once_with(20)

    def test_update_to_without_total(self):
        bar = DownloadProgressBar()
        bar.update = MagicMock()
        bar.n = 5
        bar.total = None
        bar.update_to(3, 5)
        bar.update.assert_called_once_with(10)

    def test_update_to_handles_exception(self):
        bar = DownloadProgressBar()
        bar.update = MagicMock(side_effect=Exception("fail"))
        bar.n = 0
        with self.assertRaises(Exception):
            bar.update_to(1, 1, 1)


if __name__ == "__main__":
    unittest.main()
