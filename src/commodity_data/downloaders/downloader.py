import pandas as pd

from commodity_data.downloaders import EEXDownloader, OmipDownloader, BarchartDownloader
from commodity_data.downloaders.base_downloader import BaseDownloader, FilterKeyNotFoundException
from commodity_data import logger


class CommodityDownloader:

    def __init__(self, roll_expirations: bool = True):
        dls = [a(roll_expirations) for a in (EEXDownloader, OmipDownloader, BarchartDownloader)]
        self.__downloaders = {dl.name(): dl for dl in dls}
        self.logger = logger

    def downloaders(self, market_filter: list = None) -> tuple[str, BaseDownloader]:
        market_filter = market_filter or self.__downloaders.keys()
        for market, downloader in self.__downloaders.items():
            if market in market_filter or market == market_filter:
                yield market, downloader

    def delete_data(self, ask_confirmation: bool=True):
        for market, downloader in self.downloaders():
            downloader.delete_all_data(ask_confirmation)

    def download(self, start_date: pd.Timestamp = None, end_date: pd.Timestamp=None, force_download: bool = False,
                 markets: list = None):
        for market, downloader in self.downloaders(markets):
            downloader.download(start_date, end_date, force_download=force_download)

    def settle_xs(self, allow_zero_prices: bool = True, **filter_) -> pd.DataFrame:
        data = []
        for market, downloader in self.downloaders(filter_.pop("market", None)):
            try:
                data.append(downloader.settle_xs(allow_zero_prices, **filter_))
            except FilterKeyNotFoundException as filter_exception:
                # This exception is found when trying to download data from a market that does not include
                # The given filters, ignore it
                self.logger.debug(filter_exception)
                pass
        retval = pd.concat(data)
        return retval


if __name__ == '__main__':
    downloader = CommodityDownloader()
    # This should return both Omip and EEX data
    df = downloader.settle_xs(commodity="Power", area="ES", product="Y", offset=1)
    print(df)
