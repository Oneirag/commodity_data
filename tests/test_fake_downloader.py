"""
Tests for the fake downloader
"""
import unittest

import pandas as pd

from commodity_data.downloaders.base_downloader import update_dataframe
from tests.test_downloader.fake_downloader import FakeDownloader


class TestFakeDownloader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Creates an empty downloader by removing all its data
        downloader = FakeDownloader()
        downloader.delete_all_data(do_not_ask=True)
        cls.downloader = FakeDownloader()  # create from scratch

    def test_download(self):
        """Tests data is properly downloaded"""
        self.downloader.download()
        prices = self.downloader.settlement_df
        self.assertFalse(prices.empty,
                         "No data has been downloaded")

        self.downloader.load()
        prices2 = self.downloader.settlement_df
        self.assertTrue(prices.equals(prices2),
                        "Data has changed!")

    def test_concat_downloads(self):
        """Test that data downloaded in different dates concatenates properly"""
        min_date = self.downloader.min_date()
        date1 = min_date
        date2 = min_date + pd.offsets.BDay(1)
        data1 = self.downloader._download_date(date1)
        data2 = self.downloader._download_date(date2)
        print(data1)
        print(data2)
        data = pd.DataFrame()
        if data.empty:
            data = data1
        data = update_dataframe(data, data2)
        self.assertTrue(data[data.index.isin(data2.index)][data2.columns].equals(data2))

    @classmethod
    def tearDownClass(cls):
        cls.downloader.delete_all_data(do_not_ask=True)


if __name__ == '__main__':
    unittest.main()
