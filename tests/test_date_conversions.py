import pandas as pd
import unittest

from tests.test_downloader.fake_downloader_dataframe import FakeDownloaderDataFrame


class TestDataConversions(unittest.TestCase):

    def setUp(self):
        self.dl = FakeDownloaderDataFrame(pd.DataFrame(), product="H")
        n_data = 10
        today = pd.Timestamp.today().normalize().tz_localize("Europe/Madrid")
        columns = pd.MultiIndex.from_tuples([("close",), ("maturity",)], names=['type'])
        self.dfs = dict()
        for freq in ["1h", "15min"]:
            index = pd.date_range(start=today, freq=freq, periods=n_data)
            data = [(i, d) for i, d in enumerate(index)]
            self.dfs[freq] = pd.DataFrame(data, index=index, columns=columns)

    def test_conversion(self):
        """Tests that convert to timestamp and back to datetime works, as there might be trouble with local-tz"""
        for freq, df in self.dfs.items():
            with self.subTest(freq=freq):
                ts_df = self.dl.maturity2timestamp(df)
                first_ts = ts_df.iat[0, 1]
                first_date = df.iat[0, 1]
                self.assertEqual(first_ts, first_date.timestamp(), "Conversion to timestamp did not work")
                maturity_df = self.dl.maturity2datetime(df)
                self.assertEqual(df.iat[0, 1], maturity_df.iat[0, 1], "Conversion to timestamp did not work")
                maturity_ts_df = self.dl.maturity2datetime(ts_df)
                first_maturity_ts_df = maturity_ts_df.iat[0, 1]
                print(df)
                print(maturity_ts_df)
                # self.assertTrue(df.index.equals(df.iloc[:, -1].values), "Dates do not match")
                self.assertTrue(df.equals(maturity_df), "Conversion to maturity failed")
                self.assertTrue(df.equals(maturity_ts_df), "Conversion to ts and back to datetime failed")
        pass

    def tearDown(self):
        self.dl.delete_all_data(do_not_ask=True)
