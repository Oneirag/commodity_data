"""
Tests that EEX data is properly downloaded
"""
import unittest

import pandas as pd

from commodity_data.downloaders.eex import EEXData


class EEX_Data_Test(unittest.TestCase):
    symbols = [
        "/E.FEBY",  # Spanish baseload year futures
        "/E.FEBQ",  # Spanish baseload quarter futures
        "/E.FEBM",  # Spanish baseload month futures
        "/E.FEB_WEEK",  # Spanish baseload week futures

    ]
    min_date = "2023-12-01"
    # min_date = "2024-03-01"
    max_date = "2024-03-19"

    # min_date, max_date = "2023-12-15", "2024-01-10"

    @classmethod
    def setUpClass(cls):
        cls.eex = EEXData()
        cls.dates = pd.bdate_range(cls.min_date, cls.max_date)

    def test_market_code(self):
        """Tests that correct market codes are returned for some markets"""
        expected = {
            ("EEX Austrian Power Futures", "Year", "base"): "/E.ATBY",
            ("EEX Austrian Power Futures", "Week", "peak"): "/E.ATP_WEEK",
            ("spanish", "Week", "base"): "/E.FEB_WEEK",
            ("spanish", "Week", None): "/E.FEB_WEEK",
            ("spanish", None, None): "/E.FEBY",  # Will return first appearance: Year
        }

        for key, value in expected.items():
            retval = self.eex.get_eex_symbol(*key)[0]
            self.assertEqual(retval, value)

    def test_table_vs_history(self):
        """Tests that downloading symbols day by date as a table provide the same result as downloading
        from history"""

        for symbol in self.symbols:
            tables = []
            for as_of in self.dates:
                print(f"Downloading {symbol} asof {as_of}")
                # expiration = as_of - pd.offsets.BDay(1)
                # table = self.eex.download_symbol_table(symbol=symbol, date=as_of, expiration_date=expiration)
                table = self.eex.download_symbol_chain_table(symbol=symbol, date=as_of)
                table['as_of'] = as_of
                tables.append(table)
            df_tables = pd.concat(tables)
            for price_symbol in set(df_tables['gv.pricesymbol']):
                with self.subTest(f"Testing {price_symbol}"):
                    # price_symbol = df_tables['gv.pricesymbol'].iat[0]
                    df_prices_tables = (df_tables[df_tables['gv.pricesymbol'] == price_symbol][['close', 'as_of',
                                                                                                'gv.displaydate',
                                                                                                'gv.expirationdate']]
                                        .set_index("as_of"))
                    displaydate = df_prices_tables['gv.displaydate'].iat[0]
                    expirationdate = df_prices_tables['gv.expirationdate'].iat[0]
                    history = self.eex.download_price_symbol_history(price_symbol, since=self.dates[0],
                                                                     to=self.dates[-1])
                    history = history.set_index(pd.to_datetime(history['tradedatetimegmt']).dt.normalize())
                    # Compare
                    compare_history = history['close'].dropna()
                    compare_tables = df_prices_tables['close'].dropna()
                    if expirationdate in compare_history and expirationdate not in compare_tables:
                        self.fail(f"Expiration date was not found in tables. "
                                  f"{price_symbol=} {expirationdate=} {displaydate=}")
                    comparison = compare_history.eq(compare_tables, fill_value="")
                    comparison2 = (compare_history == compare_tables).all()
                    comparison3 = compare_history.compare(compare_tables)
                    self.assertTrue(comparison.all(),
                                    f"Prices for {price_symbol} corresponding to  do not match")

    def test_price_symbol(self):
        """Test that price symbols downloaded from the market and inferred are the same"""

        reference_date = pd.Timestamp("2024-03-18")
        for symbol, maturity in [("/E.FEBY", pd.Timestamp("2025-01-01")),
                                 ("/E.FEBQ", pd.Timestamp("2024-10-01")),
                                 ("/E.FEBM", pd.Timestamp("2024-06-01"))]:
            downloaded = self.eex.get_eex_price_symbol(symbol, maturity, reference_date)
            inferred = self.eex.get_eex_price_symbol(symbol, maturity, reference_date, no_download=True)
            self.assertEqual(downloaded, inferred)


