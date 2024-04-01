"""
Class to download generic data from barchart
"""

import io
import urllib.parse

import numpy as np
import pandas as pd
from ong_utils import get_cookies

from commodity_data.common import logger
from commodity_data.downloaders.base_downloader import HttpGet
from commodity_data.downloaders.series_config import TypeColumn


class BarchartData(HttpGet):
    token_cookie = "XSRF-TOKEN"

    def __init__(self):
        super().__init__()
        self.logger = logger
        self.headers = {"Host": "www.barchart.com",
                        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0"}

    def init_connection(self, symbol: str):
        if self.cookies is None:
            url_base = f"https://www.barchart.com/futures/quotes/{symbol}/overview"
            resp = self.http_get(url_base)
            # Update cookies
            self.cookies = get_cookies(resp)
            token_header = f"x-{self.token_cookie.lower()}"
            self.headers.update({token_header: urllib.parse.unquote(self.cookies[self.token_cookie])})

    def download(self, symbol: str, start_date: pd.Timestamp, end_date: pd.Timestamp = None) -> pd.DataFrame:
        """
        Downloads a symbol price bar in DataFrame format.
        :param symbol: barchart symbol, such as CKZ27 for eua dec 27, ^ETHUSD for Ethereum...
        :param start_date: start date for download
        :param end_date: end date, defaults to today
        :return: a pandas Dataframe with  the following columns: symbol, as_of, open, high, low, close, volume
        and maybe oi
        """
        end_date = end_date or pd.Timestamp.today()
        self.init_connection(symbol)
        params = dict(symbol=symbol,
                      data="daily",
                      maxrecords=np.busday_count(start_date.date(), end_date.date()) + 1,
                      volume="contract",
                      order="asc",
                      dividends="false",
                      backadjust="false",
                      daystoexpiration=1,
                      contractroll="expiration"
                      )
        self.logger.info(f"Downloading Barchart data for {symbol}")
        resp = self.http_get("https://www.barchart.com/proxies/timeseries/queryeod.ashx", params=params)
        df_barchart = pd.read_csv(io.StringIO(resp.data.decode('utf-8')), header=None)
        df_barchart.columns = ["symbol", "as_of", "open", "high", "low",
                               TypeColumn.close.value, "volume", "oi"][:len(df_barchart.columns)]
        return df_barchart


if __name__ == '__main__':
    barchart = BarchartData()
    print(df := barchart.download("^ETHUSD", pd.Timestamp(2024, 1, 1)))
    print(df := barchart.download("CKZ27", pd.Timestamp(2024, 1, 1)))
