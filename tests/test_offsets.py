"""
Tests the offset functions
"""
import unittest

import pandas as pd

from commodity_data.downloaders.offsets import date_offset, pd_date_offset


class TestOffset(unittest.TestCase):
    def test_date_offset(self):
        """Test that date offset function works properly"""
        as_of = pd.Timestamp("2024-03-05")
        expected = [
            dict(maturity="2024-03-04", period="W", offset=0),
            dict(maturity="2024-03-11", period="W", offset=1),
            dict(maturity="2024-03-18", period="W", offset=2),
            dict(maturity="2024-03-01", period="M", offset=0),
            dict(maturity="2025-03-01", period="M", offset=12),
            dict(maturity="2024-03-01", period="Q", offset=0),
            dict(maturity="2024-04-01", period="Q", offset=1),
            dict(maturity="2024-07-01", period="Q", offset=2),
            dict(maturity="2024-01-01", period="Y", offset=0),
            dict(maturity="2025-01-01", period="Y", offset=1),
            dict(maturity="2026-01-01", period="Y", offset=2),
            # For CO2
            dict(maturity="2024-12-20", period="Y", offset=0),
            dict(maturity="2025-12-20", period="Y", offset=1),
            dict(maturity="2023-12-20", period="Y", offset=-1),
        ]

        for test_data in expected:
            with self.subTest(**(test_data | dict(as_of=as_of))) as st:
                self.assertEqual(test_data['offset'], date_offset(as_of,
                                                                  maturity=pd.Timestamp(test_data['maturity']),
                                                                  period=test_data['period']))

    def test_df_date_offset(self):
        as_of_df = pd.DataFrame(pd.date_range("2024-12-01", "2025-01-31", freq="B"), columns=["as_of"])
        as_of_df['close'] = 0

        for maturity, period in [("2024-12-15", "Y"), ("2025-12-15", "Y")]:
            as_of_df['offset'] = pd_date_offset(as_of_df.as_of.dt, pd.Timestamp(maturity),period=period)
            for as_of, offset in zip(as_of_df.as_of.values, as_of_df.offset.values):
                with self.subTest(as_of=as_of, maturity=maturity, period=period):
                    self.assertEqual(offset, date_offset(pd.Timestamp(as_of), pd.Timestamp(maturity), period=period))


if __name__ == '__main__':
    unittest.main()
