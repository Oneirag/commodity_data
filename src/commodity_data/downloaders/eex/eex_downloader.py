from datetime import date

import pandas as pd

from commodity_data.downloaders.base_downloader import BaseDownloader
from commodity_data.downloaders.eex.eex_data import EEXData
from commodity_data.downloaders.offsets import date_offset
from commodity_data.series_config import EEXConfig, df_index_columns


class EEXDownloader(BaseDownloader):
    def __init__(self, roll_expirations: bool = True):
        super().__init__("EEX", config_name="eex_downloader", class_schema=EEXConfig,
                         default_config_field="eex_downloader_use_default", roll_expirations=roll_expirations)
        self.eex = EEXData()

    def _download_date(self, as_of: pd.Timestamp) -> pd.DataFrame:
        all_tables = list()
        for cfg in self.config:
            download_cfg = cfg.download_cfg
            table = self.eex.download_symbol_chain_table(symbol=download_cfg.instrument, date=as_of)
            if table.empty:
                continue
            table['market'] = self.name()
            table['commodity'] = cfg.commodity_cfg.commodity
            table['instrument'] = cfg.commodity_cfg.instrument
            table['area'] = cfg.commodity_cfg.area
            table['product'] = cfg.download_cfg.product
            # table['type'] = "close"

            table['offset'] = table['maturity'].apply(lambda maturity: date_offset(as_of, maturity,
                                                                                   cfg.download_cfg.product,
                                                                                   ))

            if not table[table['offset'] > 20].empty:
                self.logger.warning(f"Found too big offsets for {download_cfg.instrument} as of {as_of}")

            # Convert maturities to timestamps
            table['maturity'] = table['maturity'].apply(lambda x: x.timestamp())
            # table.drop(columns=['maturity'], inplace=True)
            # table.set_index(df_index_columns, inplace=True)
            table.drop(columns=list(set(table.columns) - set(list([*df_index_columns, 'close', 'maturity']))),
                       inplace=True)
            all_tables.append(table)
        if not all_tables:
            return pd.DataFrame()
        df_retval = pd.concat(all_tables)
        df_retval['as_of'] = as_of
        df_retval = self.pivot_table(df_retval, value_columns=['close', 'maturity'])
        return df_retval

    def min_date(self):
        min_date = min(self.eex.get_min_date(cfg.download_cfg.instrument) for cfg in self.config)
        return min_date

    def get_holidays(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> dict:
        """Add custom holidays for EEX: dec24th and dec31st"""
        parent_holidays = super().get_holidays(start_date, end_date)
        # Append dec31st and dec24th
        for year in range(start_date.year, end_date.year + 1):
            parent_holidays[date(year, 12, 24)] = "Christmas Day"
            parent_holidays[date(year, 12, 31)] = "Christmas Eve"

        return parent_holidays


if __name__ == '__main__':
    force_download = False
    # force_download = True
    eex = EEXDownloader(roll_expirations=False)
    eex.delete_all_data()
    eex.download()
    exit(0)
    eex.download(start_date=pd.Timestamp(2023, 1, 1), force_download=force_download)
    # eex.download(start_date=pd.Timestamp(2024, 3, 18), force_download=True)
    # year_data = eex.settle_xs(commodity="Power", area="ES", product="Y", offset=1, type="close")
    year_data = eex.settle_xs(commodity="Power", area="ES",  # product="Y",
                              type="close",
                              maturity="2025-1-1",
                              allow_zero_prices=False)

    import matplotlib.pyplot as plt

    year_data.plot()
    plt.show()
