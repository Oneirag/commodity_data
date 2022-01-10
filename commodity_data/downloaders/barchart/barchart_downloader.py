from typing import Union

from pandas import DataFrame, Series

from commodity_data.downloaders.base_downloader import BaseDownloader, df_index_columns, TypeColumn, product_to_date, \
    combine_config
import urllib.parse
import io
import pandas as pd
import numpy as np
from commodity_data.downloaders.default_config import barchart_cfg
from ong_utils import get_cookies
from commodity_data.series_config import BarchartConfig
from commodity_data import config
import marshmallow_dataclass


class BarchartDownloader(BaseDownloader):

    def __init__(self, name: str = "Barchart"):
        """
        Creates a barchart downloader.
        For the list of symbols to read, uses a default configuration defined in
        commodity_data.downloaders.default_config.py, that can be extended using configuration file with two keys:
        - barchart_downloader: where a json/yaml configuration following BarchartConfig marshmallow spec defined in
        series_config can be used
        - barchart_downloader_replace: True/False value to define whether the configuration must extend the default
        (False, default value) or replace (if True) the available configuration
        :param name: The name of the sensor where data will be stored/read from (Barchart as a default)
        """
        cfg = config("barchart_downloader", dict())
        barchart_parser = marshmallow_dataclass.class_schema(BarchartConfig)()
        self.config = combine_config(barchart_cfg, cfg, barchart_parser,
                                     use_default=config("barchart_downloader_use_default", True))
        super().__init__(name=name)
        symbol = self.config[-1].download_cfg.symbol  # Last symbol in config
        url_base = f"https://www.barchart.com/futures/quotes/{symbol}/overview"
        self.headers = {"Host": "www.barchart.com",
                        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0"}
        resp = self.http_get(url_base)
        # Update cookies
        self.cookies = get_cookies(resp)
        self.headers.update({"x-XSRF-TOKEN".lower(): urllib.parse.unquote(self.cookies["XSRF-TOKEN"])})
        pass

    def min_date(self):
        return pd.Timestamp(2013, 1, 1)

    def download(self, start_date: pd.Timestamp = None, end_date: pd.Timestamp = None) -> int:
        # refresh cache
        if start_date is not None:
            start_date = pd.Timestamp(start_date)
        start_date = start_date or self.min_date()
        cache = dict()
        #for symbol, config in self.config.items():
        for cfg in self.config:
            symbol = cfg.download_cfg.symbol
            params = dict(symbol=symbol,
                          data="daily",
                          maxrecords=np.busday_count(start_date.date(), pd.Timestamp.today().date()) + 1,
                          volume="contract",
                          order="asc",
                          dividends="false",
                          backadjust="false",
                          daystoexpiration=1,
                          contractroll="expiration"
                          )
            self.logger.info(f"Downloading {symbol} from {self.__class__.__name__} {self.name()}")
            resp = self.http_get("https://www.barchart.com/proxies/timeseries/queryeod.ashx", params=params)
            df_barchart = pd.read_csv(io.StringIO(resp.data.decode('utf-8')), header=None)
            df_barchart.columns = ["symbol", "as_of", "open", "high", "low",
                                   TypeColumn.close.value, "volume", "oi"][:len(df_barchart.columns)]
            df_barchart.as_of = pd.to_datetime(df_barchart.as_of)
            df_barchart = df_barchart[df_barchart.as_of >= start_date]
            df = df_barchart.loc[:, ("open", "high", "low", TypeColumn.close.value, "as_of")]  # Store OHLC
            # pivot df so OHLC are split by row
            df_melt = df.melt(id_vars="as_of", var_name="type", value_name="price")
            df_melt['market'] = self.name()
            for column_name, column_value in cfg.commodity_cfg.__dict__.items():
                df_melt[column_name] = column_value
            product = cfg.download_cfg.product
            df_melt['product'] = product
            # df['type'] = TypeColumn.close.value
            expiry = cfg.download_cfg.expiry
            if expiry is not None:
                maturity = pd.to_datetime(expiry)
                df_melt['offset'] = product_to_date(maturity, product) - product_to_date(df_melt.as_of.dt, product)
            else:
                df_melt['offset'] = 0  # If no maturity, then it is supposed to be a stock or a spot value
            cache[symbol] = df_melt

        concat_df = pd.concat(cache.values(), axis=0)
        cache_df = pd.pivot_table(concat_df, values="price", index="as_of",
                                  columns=df_index_columns)
        self.cache = cache_df

        return super().download(start_date, end_date)

    def _download_date(self, as_of: pd.Timestamp) -> Union[DataFrame, Series, None]:
        if as_of in self.cache.index:
            df = self.cache.loc[[as_of]]
            return df
        else:
            return None





if __name__ == '__main__':
    ice = BarchartDownloader(barchart_cfg)
    ice.download()
    ice.settle_xs(offset=1).plot()
    import matplotlib.pyplot as plt

    plt.show()
    pass
