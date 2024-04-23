import numpy as np
import pandas as pd
from ong_esios.esios_api import EsiosApi

from commodity_data.downloaders import BaseDownloader
from commodity_data.downloaders.series_config import ESiosConfig, TypeColumn


class EsiosDownloadError(Exception):
    """Exception raised when Esios data could not be Downloaded"""
    pass


class EsiosDownloader(BaseDownloader):
    # Period will be used to create database
    period = "1h"  # Hourly
    # period = "15min"      # quarter hourly
    frequency = "1D"  # Data for every day, not just business days

    def min_date(self):
        return pd.Timestamp("2015-04-01", tz=self.local_tz)  # First day of spot indicators

    def normalize_date(self, date: pd.Timestamp, hour: int = 0, minute: int = 0) -> pd.Timestamp:
        """Returns date with the correct timezone and normalized. Optionally, with the given hour and minute"""
        retval = self.as_local_date(date).normalize()
        if hour or minute:
            retval = retval.replace(hour=hour, minute=minute)
        return retval

    def _prepare_cache(self, start_date: pd.Timestamp, end_date: pd.Timestamp, force_download: bool):
        """Downloads data from cache"""
        if self.cache is None:
            self.cache = dict()
        all_dates = pd.date_range(start_date, end_date, freq="1D")
        for config in self.download_config:
            indicator = config.download_cfg.indicator
            if indicator not in self.cache:
                dfs = list()
                for chunk in self._divide_chunks(all_dates, n=30):  # 30 days/1 month
                    download_start = self.normalize_date(chunk[0])
                    download_end = self.normalize_date(chunk[-1], hour=23, minute=45)
                    self.logger.info(f"Downloading esios data {indicator=} {download_start=} {download_end=}")
                    df = self.esios.download_by(id=indicator, date=[download_start, download_end])
                    # df = dict()
                    if df is None:
                        # There was an error in the download...retry unless we are trying a single day
                        raise EsiosDownloadError(f"Could not download data for {indicator=} {download_start=} "
                                                 f"{download_end=}")
                    else:
                        dfs.append(df)
                if dfs:
                    self.cache[indicator] = pd.concat(dfs, axis=0)
        pass

    def _download_date(self, as_of: pd.Timestamp) -> pd.DataFrame:
        maturity = TypeColumn.maturity.value
        close = TypeColumn.close.value
        tables = list()

        for cfg in self.download_config:
            cache_df = self.cache[cfg.download_cfg.indicator]
            column = cfg.download_cfg.column
            serie = cache_df[self.normalize_date(as_of):self.normalize_date(as_of, hour=23, minute=45)][column]
            table = serie.to_frame(close)  # Rename value to close
            table['market'] = self.name()
            table['commodity'] = cfg.commodity_cfg.commodity
            table['instrument'] = cfg.commodity_cfg.instrument
            table['area'] = cfg.commodity_cfg.area
            table['product'] = "H"  # H would be an hourly product. In the future, QH will be used
            table['offset'] = 0
            table[maturity] = table.index
            # Convert maturities to timestamps
            table[maturity] = table[maturity].apply(lambda x: x.timestamp())
            table = table.rename_axis("as_of").reset_index()
            pivoted = self._pivot_table(table, value_columns=[close, maturity])
            tables.append(pivoted)
        return pd.concat(tables, axis=1)

    def __init__(self, roll_expirations: bool = False):
        """Never roll expirations"""
        super().__init__(name="Esios", config_name="Esios", class_schema=ESiosConfig,
                         default_config_field="esios_downloader_use_default",
                         roll_expirations=False)
        self.esios = EsiosApi()

    @property
    def settlement_df_raw(self):
        retval = super().settlement_df
        return retval

    @property
    def settlement_df(self):
        """Returns settlement grouped daily"""
        df = self.settlement_df_raw

        def grouper(val):
            """Return mean for values that are different to maturity"""
            level_type = val.name[-1]
            if level_type == TypeColumn.maturity:
                return val.iloc[0]
            elif level_type in (TypeColumn.close, TypeColumn.adj_close):
                return np.mean(val)
            else:
                self.logger.warning(f"Unknown column type: {level_type}. Returning sum of values")
                return np.sum(val)

        if df.empty:
            return df
        retval = df.resample("1D", group_keys=True).apply(grouper)
        return retval


if __name__ == '__main__':
    esios = EsiosDownloader()
    print(esios.settlement_df)
    # esios.prepare_cache(pd.Timestamp.today().normalize(), pd.Timestamp.today().normalize(), force_download=True)
    # esios.prepare_cache(esios.min_date(), pd.Timestamp.today().normalize(), force_download=True)
    esios.delete_all_data()
    esios.download()
    # esios.download(end_date=pd.Timestamp("2015-08-01"))
    esios.download(start_date=pd.Timestamp("2023-01-01", tz=esios.local_tz))
    print(esios.settlement_df)
    esios.load()
    print(esios.settlement_df)
    from matplotlib import pyplot as plt

    df = esios.settle_xs(area="ES")
    df.plot()
    plt.show()
