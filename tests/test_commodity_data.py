"""
Tests for CommodityData
Includes tests for data consistency
"""
import unittest

from commodity_data.cdty_data import CommodityData, BaseDownloader


class TestCommodityData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cdty = CommodityData()
        cls.cdty.download(BaseDownloader.previous_days_local(3))
        cls.cdty.load()  # loads all market data

    def test_invalid_markets(self):
        """Tests that exceptions are risen on invalid markets, and not risen in valid ones"""
        valid_markets = self.cdty.markets
        invalid_markets = "ABC"
        for test_value in [None, valid_markets, [valid_markets[-2]], valid_markets[-2:]]:
            with self.subTest(valid=True, test_value=test_value):
                res = list(self.cdty.downloaders(test_value))
                if test_value is None:
                    self.assertEqual(len(res), len(valid_markets), "values wrongly filtered")
                else:
                    self.assertEqual(len(res), len(test_value), "values wrongly filtered")
        for test_value in [invalid_markets, list(invalid_markets), list(invalid_markets) + valid_markets,
                           list(invalid_markets[-1]) + valid_markets[:-2]]:
            with self.subTest(valid=False, test_value=test_value):
                with self.assertRaises(ValueError):
                    list(self.cdty.downloaders(test_value))



    def test_not_nat_maturities(self):
        """Tests that there are no Nat Maturities if prices are different from zero
        (meaning that each valid price has its maturity)"""
        all_data = self.cdty.data(None)

        for col in all_data.columns:
            if col[-1] != "maturity":
                continue
            with self.subTest(col=col):
                maturity = all_data[col]
                not_null_maturity = maturity[~maturity.isna()]
                index_close = tuple([*col[:-1], "close"])
                close = all_data[index_close]
                close_nat_maturity = close[maturity.isna()].dropna()
                self.assertTrue(close_nat_maturity.empty,
                                f"There are zero close values instead of None for {col[:-1]}. "
                                f"\nActual data starts in {not_null_maturity.index[0]}. "
                                f"\nZero values for dates: {close_nat_maturity.index}")
                self.assertTrue((close_nat_maturity == 0).all(),
                                f"Non zero close values for {col[:-1]}")
