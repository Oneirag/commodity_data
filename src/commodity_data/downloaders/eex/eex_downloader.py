import pandas as pd

from commodity_data.downloaders.base_downloader import BaseDownloader
from commodity_data.downloaders.eex.eex_data import EEXData
from commodity_data.downloaders.offsets import date_offset
from commodity_data.series_config import EEXConfig, df_index_columns


class EEXDownloader(BaseDownloader):
    def __init__(self):
        super().__init__("EEX", config_name="eex_downloader", class_schema=EEXConfig,
                         default_config_field="eex_downloader_use_default")
        self.eex = EEXData()

    def _download_date(self, as_of: pd.Timestamp) -> pd.DataFrame:
        all_tables = list()
        for cfg in self.config:
            download_cfg = cfg.download_cfg
            table = self.eex.download_symbol_chain_table(symbol=download_cfg.instrument, date=as_of)
            table['market'] = self.name()
            table['commodity'] = cfg.commodity_cfg.commodity
            table['instrument'] = cfg.commodity_cfg.instrument
            table['area'] = cfg.commodity_cfg.area
            table['product'] = cfg.download_cfg.product
            table['type'] = "close"

            table['offset'] = table['gv.displaydate'].apply(lambda maturity: date_offset(as_of, maturity,
                                                                                         cfg.download_cfg.product,
                                                                                         ))
            table.set_index(df_index_columns, inplace=True)
            table = table[['close']]
            all_tables.append(table)
        df_retval = pd.concat(all_tables)
        df_retval.T
        df_retval['as_of'] = as_of
        df_retval.set_index("as_of", inplace=True)
        return df_retval

    def min_date(self):
        min_date = min(self.eex.get_min_date(cfg.download_cfg.instrument) for cfg in self.config)
        return min_date


if __name__ == '__main__':
    # as_of_date = pd.Timestamp(2023,3, 13)
    # for product in "M", "Q":
    #     for expiry in "2023-4-1", "2023-5-1", "2023-6-1", "2023-12-1", "2024-1-1", "2024-2-1", "2024-3-1":
    #         print(expiry, date_offset(as_of_date, pd.Timestamp(expiry), product))
    # exit(0)
    for delivery, period in [("2023-03-04", "W"), ("2023-03-11", "W")]:
        print(delivery, period, date_offset(pd.Timestamp(2023, 3, 5), pd.Timestamp(delivery), period))
    eex = EEXDownloader()
    eex.download(start_date=pd.Timestamp(2024, 1, 1))
