"""
Test some omip functionalities
"""
import unittest

import pandas as pd

from commodity_data.downloaders.omip.omip_data import parse_omip_product_maturity_offset


class TestOmipFunctionality(unittest.TestCase):

    def setUp(self):
        # A dictionary of products, maturities and offsets
        self.reference_date = pd.Timestamp(2024, 3, 29)

    def test_parsed_products(self):
        """Test that parsed product are correct"""
        products = {
            pd.Timestamp(2024, 3, 29):
                {
                    "FTB YR-30": ("Y", pd.Timestamp(2030, 1, 1), 6),
                    "FTB Q4-24": ("Q", pd.Timestamp(2024, 10, 1), 3),
                    "FTB Q1-25": ("Q", pd.Timestamp(2025, 1, 1), 4),
                    "FTB Q2-25": ("Q", pd.Timestamp(2025, 4, 1), 5),
                    "FTB D We03Apr-24": ("D", pd.Timestamp(2024, 4, 3), 5),
                    "FTB D We04Apr-24": ("D", pd.Timestamp(2024, 4, 4), 6),
                    "FTB Wk15-24": ("W", pd.Timestamp(2024, 4, 8), 2),
                    "FTB Wk16-24": ("W", pd.Timestamp(2024, 4, 15), 3),
                    "FTB M Jun-24": ("M", pd.Timestamp(2024, 6, 1), 3),
                    "FTB M Jul-24": ("M", pd.Timestamp(2024, 7, 1), 4),
                    "FTB M Aug-24": ("M", pd.Timestamp(2024, 8, 1), 5),
                    "FTB WE 06Apr-24": (None, None, None),  # Weekends are not processed
                    "FTB PPA 25/29": (None, None, None),  # PPAs are not processed
                },
            pd.Timestamp(2024, 3, 20):
                {
                    "FGF WkDs13-24": (None, None, None),  # Gas weekdays (they are not weeks!)
                    "FGF Sum24": (None, None, None),  # Gas seasons
                    "FGF Win24": (None, None, None),  # Gas seasons
                    "FGF BoM Th21Mar-24": (None, None, None),  # Gas Balance of month
                    "FGF D Th21Mar-24": ("D", pd.Timestamp(2024, 3, 21), 1),
                    "FGF WE 23Mar-24": (None, None, None),  # Weekend
                    "FGF M Apr-24": ("M", pd.Timestamp(2024, 4, 1), 1),
                    "FGF M May-24": ("M", pd.Timestamp(2024, 5, 1), 2),
                    "FGF YR-25": ("Y", pd.Timestamp(2025, 1, 1, ), 1),
                }
        }
        for as_of, test_values in products.items():
            for omip_product, expected in test_values.items():
                omip_product = omip_product[4:]
                with self.subTest(omip_product=omip_product, as_of=as_of):
                    calculated = parse_omip_product_maturity_offset(omip_product, as_of)
                    self.assertSequenceEqual(calculated[:2], expected[:2])
                    # self.assertSequenceEqual(calculated, expected)
