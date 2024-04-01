"""
Test rolling functions so they work properly
"""

from unittest import TestCase, main

import numpy as np

from commodity_data.downloaders.barchart.barchart_downloader import BarchartDownloader
from commodity_data.downloaders.base_downloader import consecutive, roll
from commodity_data.downloaders.series_config import TypeColumn


def compute_incremental_pnl(prices, deals) -> tuple:
    """Calculates INCREMENTAL pnl of deals (not positions), to get total pnl a cumsum of result is needed"""
    positions = deals.cumsum()
    not_nan_prices = np.nan_to_num(prices, nan=0)  # replace with 0
    pnl = positions[1:] * np.diff(not_nan_prices)
    return pnl


class TestDownloader(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.ice = BarchartDownloader()

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
                prc2=np.array([1, 2,       3,     4, 2, 3,      4,      5, 6, 7]),
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

    def test_roll_expiration_ice(self):
        """Test that expiration on ice contracts work properly"""
        if self.ice.settlement_df.empty:
            self.ice.download()
        for roll_offset in reversed(range(10)):
            with self.subTest(roll_offset=roll_offset):
                self.ice.roll_expiration(roll_offset=roll_offset)
                self.ice.load()
                prc1 = self.ice.settle_xs(instrument="EUA", offset=1, type=TypeColumn.close.value).values.flatten()
                # prc2 SHOULDN'T have nans, so fill forward them
                prc2 = self.ice.settle_xs(instrument="EUA", offset=2, type=TypeColumn.close.value). \
                    fillna(method="ffill").values.flatten()
                adj_prc1 = self.ice.settle_xs(instrument="EUA", offset=1,
                                              type=TypeColumn.adj_close.value).values.flatten()
                idx_nan_prc1 = np.argwhere(np.isnan(prc1)).flatten()
                prc1_not_nan = prc1.copy()
                prc1_not_nan[idx_nan_prc1] = prc2[idx_nan_prc1]
                groups_nan_prc1 = consecutive(idx_nan_prc1)
                print(groups_nan_prc1)

                deals1 = np.zeros(len(prc1))
                deals2 = np.zeros(len(prc1))
                deals1[0] = 1
                for group in groups_nan_prc1:
                    roll_start_idx = group[0] - roll_offset
                    print(f"rolling at index: {roll_start_idx}")
                    deals1[roll_start_idx] = -1
                    deals2[roll_start_idx] = 1
                    roll_end_idx = min(len(deals1) - 1, group[-1] + 1 - roll_offset)
                    deals1[roll_end_idx] = 1
                    deals2[roll_end_idx] = -1
                    # roll also prices
                    prc1_not_nan[group[0] - roll_offset:group[0]] = prc2[group[0] - roll_offset:group[0]]

                adj_deals = np.zeros(len(prc1))
                adj_deals[0] = 1

                pnl_close = compute_incremental_pnl(prc1_not_nan, deals1) + compute_incremental_pnl(prc2, deals2)
                pnl_adj_close = compute_incremental_pnl(adj_prc1, adj_deals)

                cum_deals1 = deals1.cumsum()
                cum_deals2 = deals2.cumsum()
                for index, (item_pnl_close, item_pnl_adj_close) in enumerate(zip(pnl_close, pnl_adj_close)):
                    idx = index
                    print(f"Testing {idx}: {item_pnl_close:.2f} vs {item_pnl_adj_close:.2f}")
                    for offset_idx in 0, 1:  # Prints previous and next
                        idx = index + offset_idx
                        if 0 < idx < len(prc1):
                            print(f"{idx=} {roll_offset=:.2f} {prc1[idx]=:.2f} {prc2[idx]=:.2f} "
                                  f"{cum_deals1[idx]=:.2f} {cum_deals2[idx]=:.2f} {adj_prc1[idx]=:.2f}")
                    self.assertAlmostEqual(item_pnl_close, item_pnl_adj_close, places=2,
                                           msg=f"Pnl do not match in elem at index {idx} for {roll_offset=}: "
                                               f"{item_pnl_close:.2f}={item_pnl_adj_close:.2f}")
                print()  # leave empty line

    def tearDown(self) -> None:
        # Used to close sockets in client, but now client closes socket in destructor so no need to manually close them
        pass


if __name__ == '__main__':
    main()
