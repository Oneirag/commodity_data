"""
Tests for the fake downloader
"""
import pandas as pd
import unittest

from commodity_data.downloaders.base_downloader import _update_dataframe
from tests.test_downloader.fake_downloader import FakeDownloader
from tests.test_downloader.fake_downloader_dataframe import FakeDownloaderDataFrame


class TestFakeDownloader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Creates an empty downloader by removing all its data
        downloader = FakeDownloader()
        downloader.delete_all_data(do_not_ask=True)
        cls.downloader = FakeDownloader()  # create from scratch
        yesterday = FakeDownloader.today_local() - pd.offsets.Day(1)
        periods = 10
        cls.test_df_data = list(range(periods))
        cls.test_dfs = {
            "H": pd.DataFrame(cls.test_df_data,
                              index=pd.date_range(yesterday, freq="1h", periods=periods), columns=["close"]),
            "QH": pd.DataFrame(cls.test_df_data,
                               index=pd.date_range(yesterday, freq="15min", periods=periods),
                               columns=["close"]),
            "D": pd.DataFrame(cls.test_df_data, index=pd.date_range(yesterday - pd.offsets.Day(periods), freq="1D",
                                                                    periods=periods),
                              columns=["close"]),
        }
        cls.downloader_fake_df = None

    def test_download_dfs(self):
        """Test download of several dataframes"""

        for product, df in self.test_dfs.items():
            with self.subTest(product=product):
                # if product != "QH":
                #     self.skipTest("No QH")
                self.downloader_fake_df = FakeDownloaderDataFrame(df, product)
                self.downloader_fake_df.download()
                uploaded_df = self.downloader_fake_df.maturity2timestamp(self.downloader_fake_df.settlement_df)
                self.downloader_fake_df.load()
                downloaded_settle = self.downloader_fake_df.settlement_df
                print(downloaded_settle)
                downloaded_df = self.downloader_fake_df.maturity2timestamp()
                column = "index"
                self.assertTrue(uploaded_df.index.equals(downloaded_df.index),
                                f"{column=} {product=} {uploaded_df.index=} {downloaded_df.index=}")
                for idx, column in enumerate(['close', 'maturity']):
                    uploaded_value = uploaded_df.iloc[:, idx]
                    downloaded_value = downloaded_df.iloc[:, idx]
                    self.assertTrue(uploaded_value.equals(downloaded_value),
                                    f"{column=} {product=} {uploaded_value=} {downloaded_value=}")

    def test_download(self):
        """Tests data is properly downloaded"""
        self.downloader.download()
        prices = self.downloader.settlement_df
        self.assertFalse(prices.empty,
                         "No data has been downloaded")

        self.downloader.load()
        prices2 = self.downloader.settlement_df
        self.assertTrue(prices.sort_index(axis=1).equals(prices2),
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
            data = _update_dataframe(data, new_data)
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
        if cls.downloader_fake_df:
            cls.downloader_fake_df.delete_all_data(do_not_ask=True)


if __name__ == '__main__':
    unittest.main()
