import pandas as pd

from test_downloader.fake_downloader import FakeDownloader


class FakeDownloaderDataFrame(FakeDownloader):
    def __init__(self, df: pd.DataFrame, product: str):
        """
        Creates a downloader that just downloads the values of a given dataframe
        :param df: a pandas DataFrame with close and product columns
        :param frequency: D for all days, C for business days
        :param period: 1D for daily data, 1H for hourly, 15min for QH
        """
        if not df.empty:
            assert list(df.columns) == ['close'], f"Expected a dataframe just with 'close' as column"
        if not df.empty and not df.index.tz:
            raise ValueError("Indexes has no time zone")
        self.product = product.upper()
        if self.product in "YMQDW":
            self.frequency = "C"
            self.period = "1D"
        elif self.product == "H":
            self.frequency = "D"
            self.period = "1H"
        elif self.product == "QH":
            self.frequency = "D"
            self.period = "15min"
        else:
            raise ValueError(f"Invalid product: {product}")
        super().__init__()
        self.delete_all_data(do_not_ask=True)
        self.cache = df

    def min_date(self):
        return self.cache.index[0]

    def _download_date(self, as_of: pd.Timestamp) -> pd.DataFrame | None:
        rows = self.cache[as_of: as_of.replace(hour=23, minute=59)]
        if rows.empty:
            return None
        cfg = self.download_config[0].commodity_cfg.__dict__
        cfg['market'] = self.name()
        contents = [dict(close=row['close'], product=self.product, offset=0, as_of=idx, maturity=idx, **cfg)
                    for idx, row in rows.iterrows()]
        df = pd.DataFrame.from_records(contents)
        retval = self._pivot_table(df, value_columns=["close", "maturity"])
        print(retval)
        return retval


if __name__ == '__main__':
    idx = pd.date_range(pd.Timestamp.today().normalize(), freq="1h", periods=24)
    df = pd.DataFrame(range(len(idx)), index=idx, columns=["close"])
    print(df)
    fake_df = FakeDownloaderDataFrame(df, product="H")
    # fake_df.delete_all_data(do_not_ask=True)
    fake_df.download()
    fake_df.load()
    print(fake_df.settlement_df)
    fake_df.delete_all_data(do_not_ask=True)
