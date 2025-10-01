import pandas as pd

from commodity_data.downloaders import (EEXDownloader, OmipDownloader, BarchartDownloader, EsiosDownloader)
from commodity_data.downloaders.base_downloader import BaseDownloader, FilterKeyNotFoundException
from commodity_data.globals import logger


class CommodityData:
    """
    Class to download data to and query data from a OngTSDB database
    Currently reads data from EEX, Omip and Barchart
    """

    # Get static class methods related to dates from BaseDownloader
    local_tz = BaseDownloader.local_tz
    as_local_date = BaseDownloader.as_local_date
    today_local = BaseDownloader.today_local
    previous_days_local = BaseDownloader.previous_days_local

    def __init__(self, roll_expirations: bool = True,
                 downloaders=(EEXDownloader, OmipDownloader, BarchartDownloader, EsiosDownloader)):
        dls = []
        for dl in downloaders:
            try:
                dls.append(dl(roll_expirations))
            except Exception as e:
                logger.warning(f"Could not start downloader {dl.__class__.__name__} due to {e}")
        self.__downloaders = {dl.name(): dl for dl in dls}
        self.logger = logger

    def settlement_df(self, markets: str | list) -> pd.DataFrame:
        """Return a raw settlement_df of all markets"""
        list_df = list()
        for mkt, downloader in self.downloaders(markets=markets):
            list_df.append(downloader.settlement_df)
        return pd.concat(list_df)

    @property
    def markets(self) -> list:
        """Gets a list of valid downloader names (markets used in filters)"""
        return list(self.__downloaders.keys())

    def downloaders(self, markets: list | str = None) -> tuple[str, BaseDownloader]:
        """Gets an iterator of name, downloaders. Optionally: filter by given markets.
        Raises ValueError if filter is invalid"""
        if isinstance(markets, str):
            markets = [markets]
        market_filter = markets or self.markets
        if not set(market_filter).issubset(set(self.markets)):
            raise ValueError(f"{markets} filter is invalid. "
                             f"Valid filter must contain values among {','.join(self.markets)}")
        for downloader_name, downloader in self.__downloaders.items():
            if downloader_name in market_filter:
                yield downloader_name, downloader

    def delete_data(self, ask_confirmation: bool = True, markets: list = None):
        """Deletes data for the given downloaders (defaults to all of them)"""
        for market, downloader in self.downloaders(markets=markets):
            downloader.delete_all_data(not ask_confirmation)

    def download_all_yesterday(self):
        """Updates all downloaders until yesterday"""
        yesterday = pd.Timestamp.today().normalize() - pd.offsets.BDay(1)
        for mkt, downloader in self.downloaders():
            downloader.download(end_date=yesterday)

    def download(self, start_date: pd.Timestamp = None, end_date: pd.Timestamp = None,
                 force_download: bool | list | dict = False,
                 markets: str | list = None):
        """Same as BaseDownloader.downloaders, but working with all downloaders"""
        for markets, downloader in self.downloaders(markets):
            downloader.download(start_date, end_date, force_download=force_download)

    def settle_xs(self, allow_zero_prices: bool = True, markets=None, commodity=None, instrument=None, area=None,
                  product=None, offset=None, type=None, maturity=None) -> pd.DataFrame:
        """
        Applies a xs to self.settlement_df with key as values and levels as keys of filter
        :param allow_zero_prices: True (default) to leave prices=0 as 0, False to replace wthen with None
        :param markets: market from which data is downloaded (Omip, Barchart, EEX...)
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
        filter_ = dict(market=markets, commodity=commodity, instrument=instrument, area=area,
                       product=product, offset=offset, type=type, maturity=maturity)
        data = []
        for markets, downloader in self.downloaders(filter_.pop("market", None)):
            try:
                data.append(downloader.settle_xs(allow_zero_prices, **filter_))
            except FilterKeyNotFoundException as filter_exception:
                # This exception is found when trying to download data from a market that does not include
                # The given filters, ignore it
                self.logger.debug(filter_exception)
                pass
        if data:
            retval = pd.concat(data, axis=1)
        else:
            retval = pd.DataFrame()
        return retval

    def load(self, markets=None):
        """Loads data from database to memory for the given markets (all by default)"""
        for mkt, downloader in self.downloaders(markets=markets):
            downloader.load()

    def get_last_ts(self, markets: str | list = None) -> dict:
        """Returns a dict, with market name as key and the last date of its data as value"""
        retval = dict()
        for mkt, downloader in self.downloaders(markets=markets):
            retval[mkt] = downloader.date_last_data_ts()
        return retval

    def roll_expiration(self, markets: list | str = None, roll_offset: int = 0,
                        valid_products: list = None, valid_commodities: list = None,
                        valid_areas: list = None
                        ):
        """Computes roll of products in the given markets (all by default), calculating the "adj_close" column"""
        for mkt, downloader in self.downloaders(markets=markets):
            downloader.roll_expiration(roll_offset=roll_offset, valid_commodities=valid_commodities,
                                       valid_areas=valid_areas, valid_products=valid_products)

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

    def data(self, date_from: pd.Timestamp | None, markets: list | str = None) -> pd.DataFrame:
        """
        Returns all data of all markets in a single DataFrame
        :param date_from: the minimum date for reading info. Can be None to return all available data
        :param markets: optional filter to return data of just some markets
        :return: a pandas DataFrame with all data since date_from
        """
        dfs = list()
        for mkt, downloader in self.downloaders(markets):
            data = downloader.settlement_df
            if date_from:
                data = data[self.as_local_date(date_from):]
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
