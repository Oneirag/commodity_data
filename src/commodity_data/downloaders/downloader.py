import pandas as pd

from commodity_data import logger
from commodity_data.downloaders import EEXDownloader, OmipDownloader, BarchartDownloader
from commodity_data.downloaders.base_downloader import BaseDownloader, FilterKeyNotFoundException


class CommodityDownloader:

    def __init__(self, roll_expirations: bool = True):
        dls = [a(roll_expirations) for a in (EEXDownloader, OmipDownloader, BarchartDownloader)]
        self.__downloaders = {dl.name(): dl for dl in dls}
        self.logger = logger

    def downloaders(self, market: list = None) -> tuple[str, BaseDownloader]:
        """Gets an iterator of name, downloaders. Optionally: filter by given markets"""
        market_filter = market or self.__downloaders.keys()
        for market, downloader in self.__downloaders.items():
            if market in market_filter or market == market_filter:
                yield market, downloader

    def delete_data(self, ask_confirmation: bool = True):
        """Deletes data for the given downloaders (defaults to all of them)"""
        for market, downloader in self.downloaders():
            downloader.delete_all_data(ask_confirmation)

    def download_all_yesterday(self):
        """Updates all downloaders until yesterday"""
        yesterday = pd.Timestamp.today().normalize() - pd.offsets.BDay(1)
        for mkt, downloader in self.downloaders():
            downloader.download(end_date=yesterday)

    def download(self, start_date: pd.Timestamp = None, end_date: pd.Timestamp = None, force_download: bool = False,
                 markets: list = None):
        """Same as BaseDownloader.downloaders, but working with all downloaders"""
        for market, downloader in self.downloaders(markets):
            downloader.download(start_date, end_date, force_download=force_download)

    def settle_xs(self, allow_zero_prices: bool = True, **filter_) -> pd.DataFrame:
        """Same as BaseDownloader.settle_xs, but working with all downloaders"""
        data = []
        for market, downloader in self.downloaders(filter_.pop("market", None)):
            try:
                data.append(downloader.settle_xs(allow_zero_prices, **filter_))
            except FilterKeyNotFoundException as filter_exception:
                # This exception is found when trying to download data from a market that does not include
                # The given filters, ignore it
                self.logger.debug(filter_exception)
                pass
        retval = pd.concat(data, axis=1)
        return retval

    def load(self, market=None):
        """Loads data from database to memory for the given markets (all by default)"""
        for mkt, downloader in self.downloaders(market=market):
            downloader.load()

    def get_last_ts(self, market: str | list = None) -> dict:
        """Returns a dict, with market name as key and the last date of its data as value"""
        retval = dict()
        for mkt, downloader in self.downloaders(market=market):
            retval[mkt] = downloader.date_last_data_ts()
        return retval

    def roll_expiration(self, market: list | str = None, roll_offset: int = 0):
        """Computes roll of products in the given markets (all by default), calculating the "adj_close" column"""
        for mkt, downloader in self.downloaders(market=market):
            downloader.roll_expiration(roll_offset=roll_offset)


if __name__ == '__main__':
    downloader = CommodityDownloader()
    print(downloader.get_last_ts())
    downloader.download_all_yesterday()
    # downloader.roll_expiration(["EEX", "Omip"])
    # exit(0)

    # downloader.load()
    # exit(0)

    # This should return both Omip and EEX data
    df = downloader.settle_xs(commodity="Power", area="ES", product="Y", offset=1, type="close")
    df.plot()
    from matplotlib import pyplot as plt

    plt.show()
    print(df)
