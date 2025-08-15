import pandas as pd
import unittest

from commodity_data.downloaders import EEXDownloader


class TestEEXDownloader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.eex = EEXDownloader(roll_expirations=False)
        # Download data for the 3 previous days, just in case
        cls.eex.download(cls.eex.previous_days_local(2))

    def test_remove_outliers(self):
        """Test that outliers are properly removed"""
        for data, expected in [
            [(1, 2, 3, 4, 5, 6, 7), (1, 2, 3, 4, 5, 6, 7), ],
            [(1, 2, 3, 4, 5, 6, 17), (1, 2, 3, 4, 5, 6), ],
            [(-1, 1, 2, 3, 4, 5, 6, 17), (-1, 1, 2, 3, 4, 5, 6), ],
            [(-2, -1, 1, 2, 3, 4, 5, 6, 17), (-2, -1, 1, 2, 3, 4, 5, 6), ],
            [(-2, 1, 2, 3, 4, 5, 6, 17), (-2, 1, 2, 3, 4, 5, 6), ],
            [(-3, -2, 1, 2, 3, 4, 5, 6, 17), (-3, -2, 1, 2, 3, 4, 5, 6), ],
            [(-2, 1, 2, 3, 4, 5, 6, 17, 18), (-2, 1, 2, 3, 4, 5, 6), ],
            [(-4, 1, 2, 3, 4, 5, 6, 17, 18, 35), (-4, 1, 2, 3, 4, 5, 6), ],
        ]:
            orig_df = pd.DataFrame(data=data, columns=["offset"])
            clean_df = self.eex.remove_outliers(orig_df, "fake", pd.Timestamp.now())
            self.assertSequenceEqual(list(expected), clean_df['offset'].values.tolist())

    def test_fix_maturity(self):
        """Test that maturities are properly modified from ts to datetime"""
        # df Maturity initially should be timestamps
        self.eex.maturity2datetime()
        df = self.eex.settlement_df
        if df.empty:
            self.fail("No EEX data. Please download some data EEX")

        # self.eex.maturity2datetime()
        maturity = df.xs("maturity", level="type", axis=1)
        self.assertTrue(all(self.eex.check_dtype("datetime", dtype) for dtype in maturity.dtypes),
                        "Multilevel indexes are not datetime")
        # Checks that no 1970 dates are there:
        for col in maturity.columns:
            self.assertTrue((maturity[col].dropna().dt.year > 1970).all(),
                            msg=f"Bad conversion of dates: There are 1970 dates in column {col}")
        self.eex.maturity2timestamp()
        df = self.eex.settlement_df
        maturity = df.xs("maturity", level="type", axis=1)
        self.assertTrue((maturity.dtypes == float).all(),
                        "Multilevel indexes are not float")
        # Checks that no nan timestamps:
        for col in maturity.columns:
            self.assertTrue((maturity[col].dropna() > 0).all(),
                            msg=f"Bad conversion of dates: There are 0 timestamps in column {col}")
        pass

    def test_force_download_filter(self):
        """Test that force download filter works properly"""
        all_configs = list(self.eex._iter_download_config())
        self.assertTrue(len(all_configs) > 0, "No download config returned")
        # Boolean: all configs must be returned always for True, False (and None also)
        for bool_filter in True, False, None:
            self.eex.set_force_download_filter(bool_filter)
            bool_configs = list(self.eex._iter_download_config())
            self.assertTrue(len(all_configs) == len(bool_configs), "Expected all configs")
        # If filter is not valid should raise exception
        with self.assertRaises(Exception) as ar:
            self.eex.set_force_download_filter(dict(non_existing_field=True))
            configs = list(self.eex._iter_download_config())
        print(f"Raised: {ar.exception}")
        # Single filters: a dict that returns just one config
        for simple_filters in [dict(instrument="/E.FEBY"), dict(product="Y", instrument="/E.FEBY"),
                               [dict(instrument="/E.FEBY"), dict(product="Y", instrument="/E.FEBY")]
                               ]:
            self.eex.set_force_download_filter(simple_filters)
            configs = list(self.eex._iter_download_config())
            self.assertTrue(len(configs) == 1, "Expected one single config")

        for impossible_filters in [dict(product="Q", instrument="/E.FEBY"), ]:
            self.eex.set_force_download_filter(impossible_filters)
            configs = list(self.eex._iter_download_config())
            self.assertTrue(len(configs) == 0, "Expected no configs")

        for double_filters in [[dict(instrument="/E.FEBY"), dict(instrument="/E.FEBQ")]]:
            self.eex.set_force_download_filter(double_filters)
            configs = list(self.eex._iter_download_config())
            self.assertTrue(len(configs) == 2, "Expected two configs")
