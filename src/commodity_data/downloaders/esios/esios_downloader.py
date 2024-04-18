import numpy as np
import pandas as pd
from ong_utils import OngTimer

from commodity_data.downloaders import BaseDownloader
from commodity_data.downloaders.series_config import ESiosConfig, TypeColumn
from ong_esios.esios_api import EsiosApi


class EsiosDownloader(BaseDownloader):
    # Period will be used to create database
    period = "1H"  # Hourly
    # period = "15min"      # quarter hourly
    local_tz = "Europe/Madrid"
    frequency = "1D"  # Data for every day

    def get_holidays(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> dict:
        """Returns no holidays"""
        return dict()

    def min_date(self):
        return pd.Timestamp("2015-04-01")  # First day of spot indicators

    def normalize_date(self, date: pd.Timestamp, hour: int = 0, minute: int = 0) -> pd.Timestamp:
        """Returns date with the correct timezone and normalized. Optionally, with the given hour and minute"""
        retval = date.tz_localize(self.local_tz).normalize()
        if hour or minute:
            retval = retval.replace(hour=hour, minute=minute)
        return retval

    def prepare_cache(self, start_date: pd.Timestamp, end_date: pd.Timestamp, force_download: bool):
        if self.cache is None:
            self.cache = dict()
        for config in self.download_config:
            indicator = config.download_cfg.indicator
            if indicator not in self.cache:
                dfs = list()
                dates = pd.date_range(start_date, end_date, freq="1D")
                chunks = int(len(dates) / 100) + 1  # 23s
                # chunks = int(len(dates) / 200) + 1  # 41 s
                # chunks = int(len(dates) / 50) + 1   # 34s
                # chunks = int(len(dates) / 150) + 1   # 36s
                # chunks = int(len(dates) / 80) + 1   # 45s
                with OngTimer(msg="Downloading from esios"):
                    for as_of_chunk in np.array_split(dates, chunks):
                        start_date = self.normalize_date(as_of_chunk[0])
                        end_date = self.normalize_date(as_of_chunk[-1], hour=23, minute=45)
                        self.logger.info(f"Downloading esios data {indicator=} {start_date=} {end_date=}")
                        df = self.esios.download_by(id=indicator, date=[start_date, end_date])
                        dfs.append(df)
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
            pivoted = self.pivot_table(table, value_columns=[close, maturity])
            tables.append(pivoted)
        return pd.concat(tables, axis=0)

    def __init__(self, roll_expirations: bool = False):
        """Never roll expirations"""
        super().__init__(name="Esios", config_name="Esios", class_schema=ESiosConfig,
                         default_config_field="esios_downloader_use_default",
                         roll_expirations=False)
        self.esios = EsiosApi()


if __name__ == '__main__':
    esios = EsiosDownloader()
    esios.delete_all_data()
    # esios.download(end_date=pd.Timestamp("2015-08-01"))
    esios.download(start_date=pd.Timestamp("2024-04-01"))
    print(esios.settlement_df)
