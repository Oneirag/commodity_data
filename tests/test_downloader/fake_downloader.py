"""
Test downloader: creates a fake downloader for testing
"""
import pandas as pd
from dataclasses import dataclass

from commodity_data.downloaders.base_downloader import BaseDownloader
from commodity_data.downloaders.series_config import CommodityCfg, _BaseDownloadConfig


@dataclass
class _FakeDownloadConfig(_BaseDownloadConfig):
    id: str


@dataclass
class FakeConfig:
    commodity_cfg: CommodityCfg
    download_cfg: _FakeDownloadConfig

    def id(self) -> str:
        return self.download_cfg.id


class FakeDownloader(BaseDownloader):
    """A class that creates an empty database and fills it with fake data for testing.
    It is meant to be created and destroyed after creation"""

    def min_date(self):
        data_days = 3
        retval = self.today_local() - pd.offsets.BDay(data_days)
        return retval

    def generate_fake_data(self, as_of: pd.Timestamp, add_spot: bool = True, add_day_ahead: bool = True,
                           add_month_ahead: bool = True, price_offset=0):
        data = list()
        cfg = self.download_config[0].commodity_cfg.__dict__
        cfg['market'] = self.name()

        if add_spot:
            data.append(dict(maturity=as_of, offset=0, close=10 + price_offset, product="D", as_of=as_of, **cfg))
        if add_day_ahead:
            data.append(dict(maturity=as_of + pd.offsets.Day(1), offset=1, close=20 + price_offset, product="D",
                             as_of=as_of, **cfg))
        if add_month_ahead:
            data.append(dict(maturity=as_of + pd.offsets.MonthBegin(1), offset=1, close=30 + price_offset,
                             product="M", as_of=as_of, **cfg))
        df = pd.DataFrame.from_records(data)
        retval = self._pivot_table(df, value_columns=["close", "maturity"])

        return retval

    def _download_date(self, as_of: pd.Timestamp) -> pd.DataFrame:
        if as_of == self.min_date():
            df = self.generate_fake_data(as_of, add_spot=False, add_day_ahead=False, add_month_ahead=True,
                                         price_offset=1)
        else:
            df = self.generate_fake_data(as_of, add_spot=True, add_day_ahead=True, add_month_ahead=False)
        return df

    def __init__(self, roll_expirations: bool = False):
        super().__init__("fake", None, None, None,
                         roll_expirations=roll_expirations)

    def _create_config(self, config_field: str, class_schema, default_config_field: str):
        cfg = FakeConfig(commodity_cfg=CommodityCfg(commodity="fake_commodity", instrument="fake_instrument",
                                                    area="fake_area"),
                         download_cfg=_FakeDownloadConfig(id="fake_id"))
        return [cfg]


if __name__ == '__main__':
    # td = FakeDownloader(False)
    # td.delete_all_data(do_not_ask=True)
    td = FakeDownloader()
    td.delete_all_data(do_not_ask=True)
    td.download()
    td.load()
    print(td.settlement_df)
    td.delete_all_data(do_not_ask=True)
