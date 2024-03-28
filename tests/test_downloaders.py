import unittest

import pandas as pd

from commodity_data.downloaders import EEXDownloader


class TestEEXDownloader(unittest.TestCase):

    def setUp(self):
        self.eex = EEXDownloader(roll_expirations=False)

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

        # self.eex.maturity2datetime()
        maturity = df.xs("maturity", level="type", axis=1)
        self.assertTrue((maturity.dtypes == "datetime64[ns]").all(),
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
