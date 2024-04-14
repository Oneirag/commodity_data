"""
Test rolling functions so they work properly
"""

from unittest import TestCase, main

import numpy as np
import pandas as pd

from commodity_data import CommodityData
from commodity_data.downloaders.continuous_prices import calculate_continuous_prices, roll, consecutive
from commodity_data.downloaders.series_config import TypeColumn


def pandas_fill(arr):
    df = pd.DataFrame(arr.copy())
    df.ffill(axis=0, inplace=True)
    out = df.values.flatten()
    return out


def compute_incremental_pnl(prices, deals) -> tuple:
    """Calculates INCREMENTAL pnl of deals (not positions), to get total pnl a cumsum of result is needed"""
    positions = deals.cumsum()
    # not_nan_prices = np.nan_to_num(prices, nan=0)  # replace with 0
    not_nan_prices = pandas_fill(prices)  # fill forward
    pnl = positions[1:] * np.diff(not_nan_prices)
    return pnl


class TestDownloader(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.downloader = CommodityData()

    def test_roll_simple(self):
        """Tests a battery of simple rollings"""
        tests = [
            # First case: at expiration prc1 and prc2 are equal, so no change
            dict(
                prc1=np.array([1, 2, np.nan, np.nan, 1, 2]),
                prc2=np.array([1, 2, 3, 4, 2, 3]),
                expiry=np.array([3]),
                offset=0,
                expected_result=[1, 2, 3, 4, 1, 2],
            ),
            # Second case: at expiration prc1 and prc2 are non-equal
            dict(
                prc1=np.array([1, 2, np.nan, np.nan, 1, 2]),
                prc2=np.array([1, 3, 3, 4, 2, 3]),
                expiry=np.array([3]),
                offset=0,
                expected_result=[1, 2, 2, 3, 0, 1],
            ),
            # Third case: there are no nans, roll must be done exactly at expiration
            dict(
                prc1=np.array([1, 2, 2, 2, 1, 2]),
                prc2=np.array([1, 3, 3, 4, 2, 3]),
                expiry=np.array([3]),
                offset=0,
                expected_result=[1, 2, 2, 2, -1, 0],
            ),
            # Forth case: at expiration prc1 and prc2 are non-equal, but prc1 keeps being nan after expiration so
            # roll must be done also after expiration
            dict(
                prc1=np.array([1, 2, np.nan, np.nan, np.nan, 2]),
                prc2=np.array([1, 3, 3, 4, 2, 3]),
                expiry=np.array([3]),
                offset=0,
                expected_result=[1, 2, 2, 3, 1, 1],
            ),
            # Fifth case: dual expiration
            dict(
                prc1=np.array([1, 2, np.nan, np.nan, 1, 2, np.nan, np.nan, 1, 2]),
                prc2=np.array([1, 2, 3, 4, 2, 3, 4, 5, 6, 7]),
                expiry=np.array([3, 7]),
                offset=0,
                expected_result=[1, 2, 3, 4, 1, 2, 3, 4, 0, 1],
            ),

        ]
        for idx, test in enumerate(tests):
            prc_roll = roll(test['prc1'], test['prc2'], test['expiry'], test['offset'])
            self.assertSequenceEqual(prc_roll.tolist(),
                                     test['expected_result'],
                                     msg=f"Failed in test case {idx}: {test}")
            print(f"Test case {idx} OK")

    def test_roll_expiration_omip(self):
        """Tests that adj_close values in Omip are valid, e.g have no too many nans"""
        market = "Omip"
        if not self.downloader.get_last_ts(markets=market):
            self.downloader.download(markets=market)
        for roll_offset in reversed(range(1)):
            settlement_df = self.downloader.settlement_df(markets=market)
            settlement_df = settlement_df.drop("adj_close", level="type", axis=1, errors="ignore")
            rolled = calculate_continuous_prices(
                settlement_df, valid_products=["Y", "W"], roll_offset=roll_offset
            )
            filter_roll = dict(commodity="Power", product="Y", offset=1, area="ES", type="adj_close")
            prompt_year = rolled.xs(tuple(filter_roll.values()), level=tuple(filter_roll.keys()), axis=1,
                                    drop_level=False)
            # Remove first year, as there was not y+2 data during 2007
            start_year = 2007
            number_of_nans = (prompt_year[start_year:].isna().sum()).iat[0]
            # Allow 1 nan per year as max
            self.assertLessEqual(number_of_nans, prompt_year.index[-1].year - start_year,
                                 "Found too many nan values in prompt year"
                                 )

    def check_roll_expiration(self, market: str, commodity: str, product: str, area: str,
                              instrument: str, roll_offset: int, min_date: pd.Timestamp = None,
                              max_date: pd.Timestamp = None, skip_dates: list = None):
        skip_dates = skip_dates or list()
        skip_dates = list(pd.Timestamp(dt) for dt in skip_dates)
        common_filter = dict(markets=market, commodity=commodity, product=product, area=area, instrument=instrument)
        df_offset = self.downloader.settle_xs(**common_filter, offset=[1, 2], type=TypeColumn.close.value)
        if df_offset.empty:
            return self.skipTest(f"No data available for {common_filter}")
        dates = df_offset.index
        max_date = max_date or dates[-1]
        if min_date is None:
            # If no min date, it will be 5 years ago
            min_date = max(dates[0], pd.Timestamp.today().normalize() - pd.offsets.YearBegin(5))
        dates = df_offset[min_date:max_date].index
        prc_offset_1 = df_offset[min_date:max_date].values[:, 0]
        prc_offset_2 = df_offset[min_date:max_date].values[:, 1]
        prc_adj_1 = self.downloader.settle_xs(**common_filter, offset=1,
                                              type=TypeColumn.adj_close.value)[
                    min_date:max_date].values.flatten()
        idx_nan_prc1 = np.argwhere(np.isnan(prc_offset_1)).flatten()
        prc1_not_nan = prc_offset_1.copy()
        prc2_not_nan = pandas_fill(prc_offset_2)
        prc1_not_nan[idx_nan_prc1] = prc_offset_2[idx_nan_prc1]
        groups_nan_prc1 = consecutive(idx_nan_prc1)
        print(groups_nan_prc1)

        deals1 = np.zeros(len(prc_offset_1))
        deals2 = np.zeros(len(prc_offset_1))
        deals1[0] = 1
        for group in groups_nan_prc1:
            if len(group) == 0:
                continue
            group = [g for g in group if not (pd.isna(prc_offset_1[g]) and pd.isna(prc_offset_2[g]))]
            if not group:
                # If price is NaN for both offsets skip this group because it can be a non captured holiday
                continue
            roll_start_idx = group[0] - roll_offset
            # if roll_offset == 1088:
            #     continue
            print(f"rolling at index: {roll_start_idx} date {dates[roll_start_idx]}")
            deals1[roll_start_idx] = -1
            deals2[roll_start_idx] = 1
            roll_end_idx = min(len(deals1) - 1, group[-1] + 1 - roll_offset)
            deals1[roll_end_idx] = 1
            deals2[roll_end_idx] = -1
            # roll also prices
            prc1_not_nan[group[0] - roll_offset:group[0]] = prc_offset_2[group[0] - roll_offset:group[0]]
            prc1_not_nan[group[0] - roll_offset:group[0]] = prc2_not_nan[group[0] - roll_offset:group[0]]

        adj_deals = np.zeros(len(prc_offset_1))
        adj_deals[0] = 1

        pnl_close = compute_incremental_pnl(prc1_not_nan, deals1) + compute_incremental_pnl(prc_offset_2,
                                                                                            deals2)
        pnl_adj_close = compute_incremental_pnl(prc_adj_1, adj_deals)

        cum_deals1 = deals1.cumsum()
        cum_deals2 = deals2.cumsum()
        for index, (item_pnl_close, item_pnl_adj_close, timestamp) in enumerate(zip(pnl_close,
                                                                                    pnl_adj_close,
                                                                                    dates)):
            if timestamp in skip_dates:
                continue
            idx = index
            date = timestamp.isoformat()[:10]
            print(f"Testing {idx} {date}: {item_pnl_close:.2f} vs {item_pnl_adj_close:.2f}")
            for offset_idx in 0, 1:  # Prints previous and next
                idx = index + offset_idx
                if 0 < idx < len(prc_offset_1):
                    print(f"{idx=} {date=} {roll_offset=:.2f} {prc_offset_1[idx]=:.2f} "
                          f"{prc_offset_2[idx]=:.2f} "
                          f"{cum_deals1[idx]=:.2f} {cum_deals2[idx]=:.2f} {prc_adj_1[idx]=:.2f}")
            if pd.isna(item_pnl_close) and pd.isna(item_pnl_adj_close):
                continue
            self.assertAlmostEqual(item_pnl_close, item_pnl_adj_close, places=2,
                                   msg=f"Pnl do not match in elem at index {idx} ({date})"
                                       f" for {roll_offset=}: {item_pnl_close:.2f}!={item_pnl_adj_close:.2f}")
        print()  # leave empty line

    def test_roll_expiration(self):
        """Check expiration of Cal+1, and Q+1 and M+1 of power and gas in Omip and EEX"""
        min_date = pd.Timestamp("2020-01-01")
        # min_date = pd.Timestamp("2024-01-01")
        # This test Does not work with M:
        # It uses NaNs to detect end of month, but M+1 product might not have NaNs before end of delivery
        # However, if change of maturity dates would be used, it would work properly but that would be the exact code
        # to test
        products = ["Y", "Q"]
        # products = ["Q"]
        for market, commodity, area, instrument in [
            ("Omip", "Power", "ES", "BL"),
            ("Omip", "Power", "FR", "BL"),
            ("Omip", "Power", "DE", "BL"),
            ("Omip", "Gas", "ES", "BL"),
            ("EEX", "Power", "ES", "BL"),
            ("Barchart", "EUA", "EU", "BL")
        ]:
            for roll_offset in reversed(range(1)):
                self.downloader.roll_expiration(markets=market, roll_offset=roll_offset, valid_products=products,
                                                valid_areas=[area], valid_commodities=[commodity])
                self.downloader.load(markets=market)
                for product in products:
                    with self.subTest(market=market, commodity=commodity, product=product, min_date=min_date,
                                      area=area, roll_offset=roll_offset):
                        self.check_roll_expiration(market=market, commodity=commodity, product=product,
                                                   area=area, instrument=instrument, min_date=min_date,
                                                   roll_offset=roll_offset)

    def tearDown(self) -> None:
        # Used to close sockets in client, but now client closes socket in destructor so no need to manually close them
        pass


if __name__ == '__main__':
    main()
