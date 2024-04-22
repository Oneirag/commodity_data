import pandas as pd
from pandas import DataFrame, Series
from typing import Union

from commodity_data.downloaders.barchart.barchart_data import BarchartData
from commodity_data.downloaders.base_downloader import BaseDownloader, TypeColumn
from commodity_data.downloaders.products import pd_date_offset
from commodity_data.downloaders.series_config import BarchartConfig


class BarchartDownloader(BaseDownloader):

    def __init__(self, roll_expirations: bool = True):
        """
        Creates a barchart downloader.
        For the list of symbols to read, uses a default configuration defined in
        commodity_data.downloaders.default_config.py, that can be extended using configuration file with two keys:
        - barchart_downloader: where a json/yaml configuration following BarchartConfig marshmallow spec defined in
        series_config can be used
        - barchart_downloader_replace: True/False value to define whether the configuration must extend the default
        (False, default value) or replace (if True) the available configuration
        """
        super().__init__(name="Barchart", config_name="barchart_downloader", class_schema=BarchartConfig,
                         default_config_field="barchart_downloader_use_default", roll_expirations=roll_expirations)
        self.data = BarchartData()

    def min_date(self):
        return pd.Timestamp(2013, 1, 1)

    def _prepare_cache(self, start_date: pd.Timestamp, end_date: pd.Timestamp, force_download: bool):
        cache = dict()
        for cfg in self._iter_download_config():
            symbol = cfg.download_cfg.symbol
            df_barchart = self.data.download(symbol, start_date=start_date, end_date=end_date)
            df_barchart.as_of = pd.to_datetime(df_barchart.as_of).dt.tz_localize(self.local_tz)
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
                df_melt['maturity'] = maturity.timestamp()
                df_melt['offset'] = pd_date_offset(df_melt.as_of.dt, maturity=maturity, product=product)
            else:
                df_melt['maturity'] = df_melt.as_of.apply(lambda dt: dt.timestamp())
                df_melt['offset'] = 0  # If no maturity, then it is supposed to be a stock or a spot value
            cache[symbol] = df_melt

        concat_df = pd.concat(cache.values(), axis=0)
        concat_df.rename(columns={"price": "close"}, inplace=True)
        cache_df = self._pivot_table(concat_df, value_columns=['close', 'maturity'])
        self.cache = cache_df

    def _download_date(self, as_of: pd.Timestamp) -> Union[DataFrame, Series, None]:
        if as_of in self.cache.index:
            df = self.cache.loc[[as_of]]
            return df
        else:
            return None


if __name__ == '__main__':
    ice = BarchartDownloader()
    ice.download()
    ice.settle_xs(offset=1).plot()
    import matplotlib.pyplot as plt

    plt.show()
    pass
