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
        dates = pd.date_range(min_date, periods=3, freq="1B")
        generated_data = [
            self.downloader._download_date(dates[0]),
            self.downloader._download_date(dates[1]),
            self.downloader.generate_fake_data(dates[2], add_spot=False, add_day_ahead=False, add_month_ahead=True)
        ]
        print(generated_data)
        data = pd.DataFrame()
        for new_data in generated_data:
            data = update_dataframe(data, new_data)
        print(data)
        for data_comp in generated_data:
            with self.subTest(date=data_comp.index):
                data_as_of = data[data.index.isin(data_comp.index)]
                self.assertTrue(data_as_of[data_comp.columns].equals(data_comp),
                                f"Failed: data downloaded does not match data merged for {data_comp.index}")
                self.assertTrue(data_as_of[data.columns.difference(data_comp.columns)].isna().all().all(),
                                f"Failed: data not downloaded is not null for {data_comp.index}")

    @classmethod
    def tearDownClass(cls):
        cls.downloader.delete_all_data(do_not_ask=True)


if __name__ == '__main__':
    unittest.main()
