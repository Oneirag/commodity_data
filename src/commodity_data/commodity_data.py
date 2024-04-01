import pandas as pd

from commodity_data.downloaders import EEXDownloader, OmipDownloader, BarchartDownloader
from commodity_data.downloaders.base_downloader import BaseDownloader, FilterKeyNotFoundException
from commodity_data.globals import logger


class CommodityData:
    """
    Class to download data to and query data from a OngTSDB database
    Currently reads data from EEX, Omip and Barchart
    """

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

    def download(self, start_date: pd.Timestamp = None, end_date: pd.Timestamp = None,
                 force_download: bool | list | dict = False,
                 markets: str | list = None):
        """Same as BaseDownloader.downloaders, but working with all downloaders"""
        for market, downloader in self.downloaders(markets):
            downloader.download(start_date, end_date, force_download=force_download)

    def settle_xs(self, allow_zero_prices: bool = True, market=None, commodity=None, instrument=None, area=None,
                  product=None, offset=None, type=None, maturity=None) -> pd.DataFrame:
        """
        Applies a xs to self.settlement_df with key as values and levels as keys of filter
        :param allow_zero_prices: True (default) to leave prices=0 as 0, False to replace wthen with None
        :param market: market from which data is downloaded (Omip, Barchart, EEX...)
        :param commodity: Generic name of commodity (Power, Gas, CO2....)
        :param instrument: BL (baseload)/PK (peak load), EUA...
        :param area: Country (two caps letters, ES, FR, DE, EU...)
        :param product: # D/W/M/Q/Y for calendar day/week/month/quarter/year
        :param offset: # Number of calendar products of interval from as_of date till maturity
        :param type: "close", "adj_close" mainly. Could be also 'maturity'
        :param maturity: date for filtering maturity to a specific date. It will be converted with pd.Timestamp.
         So far, it cannot be used together with offset
        :return: a filtered dataframe
        """
        filter_ = dict(market=market, commodity=commodity, instrument=instrument, area=area,
                       product=product, offset=offset, type=type, maturity=maturity)
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

    def data_stack(self, date_from: pd.Timestamp | None, market: list | str = None) -> pd.DataFrame:
        """Same as data(), but returns a stacked pandas dataframe (with values of type level as columns and the rest
        of the levels of the multiindex columns transferred to the index).
        This way is easy to export to a plain text file or to a standard SQL database"""
        full_data = self.data(date_from, market)
        full_data.index.names = ["as_of"]  # Otherwise its name will be None when converted to multiindex after stack
        # Stacks all levels but the last one (which is the "type")
        stack_levels = list(i for i, level in enumerate(full_data.columns.names) if level != "type")
        full_data_stacked = full_data.stack(level=stack_levels, future_stack=True)
        # Maturity should be the first column, so it is next to the offset
        full_data_stacked = full_data_stacked[["maturity"] + [c for c in full_data_stacked.columns if c != "maturity"]]
        # Remove emtpy maturities and sort index
        retval = full_data_stacked[~full_data_stacked['maturity'].isna()].sort_index()
        return retval

    def data(self, date_from: pd.Timestamp | None, market: list | str = None) -> pd.DataFrame:
        """
        Returns all data of all markets in a single DataFrame
        :param date_from: the minimum date for reading info. Can be None to return all available data
        :param market: optional filter to return data of just some markets
        :return: a pandas DataFrame with all data since date_from
        """
        dfs = list()
        for mkt, downloader in self.downloaders(market):
            data = downloader.settlement_df
            if date_from:
                data = data[date_from:]
            dfs.append(data)
        pass
        if dfs:
            return pd.concat(dfs, axis=1)
        else:
            return pd.DataFrame()


if __name__ == '__main__':
    downloader = CommodityData()
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
