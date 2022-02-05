import abc
from commodity_data import http, logger, config
from ong_tsdb.client import OngTsdbClient
from ong_utils import is_debugging, cookies2header
import numpy as np
import pandas as pd
import holidays
from commodity_data.series_config import df_index_columns, TypeColumn
import multiprocessing.pool

pd.options.mode.chained_assignment = 'raise'  # Raises SettingWithCopyWarning error instead of just warning


def product_to_date(obj, product: str):
    """Transforms product as string (Y/M/Q/D) into functions call to datetime objects"""
    freqs = dict(Y="year",
                 Q="quarter",
                 M="month",
                 D="day")
    return getattr(obj, freqs[product])


def combine_config(default_config: list, config: list, parser, use_default: bool=True) -> list:
    """
    Returns a list of parsed values from a combination of default_config and config, depending on parameters
    :param default_config: required, the default configuration (a list of dicts)
    :param config: optional list of dicts. If null, default_config will be used.If not null, behaviour depends
    on the vaules of replace param
    :param parser: a parser to convert dict elements of list into objects
    :param use_default: if False, values in config will be used and default_config will be ignored. If True (default),
    values of config will overwrite values in default_config that matches the same id()
    :return: a list of parsed configuration
    """
    """"""
    # No additional config, use default
    if config is None:
        return [parser.load(item) for item in default_config]
    # A configuration to replace everything, use config and ignore default_config
    if not use_default:
        return [parser.load(item) for item in config]
    # Mix both configs
    list_default = [parser.load(item) for item in default_config]
    list_config = [parser.load(item) for item in config]
    dict_default = {item.id(): item for item in list_default}
    dict_config = {item.id(): item for item in list_config}
    # Return combination overwriting values defined in default config with the ones in config
    return list({**dict_default, **dict_config}.values())


class BaseDownloader:
    period = "1D"
    database = "commodity_data"

    def __init__(self, name: str):
        """
        Initializes the Downloader, creating clients using configuration. It needs url, host, admin_token, write_token
        and read_token keys
        :param name: Name of the sensor that will be created to store data
        """
        self.__name = name
        self.http = http
        self.date_format = "%Y-%m-%d"
        # Headers for http gets, child classes should define a method for updating it
        self.headers = None
        # Cookies for http gets, child classes should define a method for updating it
        self.cookies = None
        self.logger = logger
        # Configuration of ong_tsdb database
        self._db_client_admin = OngTsdbClient(config("url"), config("admin_token"))
        self._db_client_write = OngTsdbClient(config("url"), config("write_token"))
        if not self._db_client_admin.exist_db(self.database):
            self._db_client_admin.create_db(self.database)
        if not self._db_client_admin.exist_sensor(self.database, self.name()):
            self._db_client_admin.create_sensor(self.database, self.name(), self.period, [],
                                                config("read_token"), config("write_token"),
                                                level_names=df_index_columns)
        else:
            if not self._db_client_admin.get_metadata(self.database, self.name()):
                self._db_client_admin.set_level_names(self.database, self.name(), df_index_columns)
        self._db_client_admin.config_reload()  # Forces config reload in case external changes found
        self.__settlement_df = None
        self.cache = None
        self.last_data_ts = self.date_last_data_ts()
        pass

    @property
    def settlement_df(self):
        if self.__settlement_df is None:
            self.load()
        return self.__settlement_df

    def date_last_data_ts(self):
        """Returns last date (for any data in current database)"""
        # Admin client must be used, as it fails if sensor does not exist so there are no permissions for getting date
        last_date = self._db_client_admin.get_lastdate(self.database, self.name())
        if last_date is None:
            return None
        return last_date

    @abc.abstractmethod
    def min_date(self):
        """Returns minimum date for downloading, that will be last data downloaded"""
        if self.last_data_ts is None:
            self.last_data_ts = self.date_last_data_ts()
        return self.last_data_ts

    def name(self) -> str:
        """Returns the name of the origin"""
        return self.__name

    def settle_xs(self, **filter_):
        """Applies a xs to self.settlement_df with key as values and levels as keys of filter"""
        try:
            return self.settlement_df.xs(key=tuple(filter_.values()), level=tuple(filter_.keys()), axis=1,
                                         drop_level=False)
        except KeyError as ke:
            # the key not found
            failed_key = ke.args[0]
            # level of the not found key
            failed_level = [k for (k, v) in filter_.items()
                            if (failed_key in v if isinstance(v, (list, tuple)) else failed_key == v)][0]
            # the values available in the failed level
            values_failed_level = self.settlement_df.columns.unique(failed_level).values
            # the level (if any) in which key was found
            level_failed_key = list(v.name for v in
                                    (self.settlement_df.columns.unique(l) for l in self.settlement_df.columns.names)
                                    if failed_key in v)
            raise ValueError(f"Key {failed_key} not found in level '{failed_level}' "
                             f"with available values {values_failed_level}. "
                             f"Key was found in level {level_failed_key}") from None

    def download(self, start_date: pd.Timestamp = None, end_date: pd.Timestamp = None) -> int:
        """
        Downloads and stores data from a start date to an end date
        :param start_date:
        :param end_date:
        :return: the number of downloaded days
        """
        retval = 0
        start_date = pd.Timestamp(start_date or self.min_date())
        end_date = pd.Timestamp(end_date or pd.Timestamp.today().normalize())
        ecb_hols = holidays.EuropeanCentralBank(years=range(start_date.year, end_date.year + 1))
        as_of_dates = pd.bdate_range(start_date, end_date, holidays=ecb_hols, freq="C")
        # Chunked in months (20 Business days aprox)
        for as_of_chunk in np.array_split(as_of_dates, 20):
            # In case of debugging, don't use multiprocessing
            # map_func = map if self.cache is not None or is_debugging() else multiprocessing.Pool(4).map
            map_func = map if self.cache is not None or is_debugging() else multiprocessing.pool.ThreadPool(4).map
            # map_func = map      # No multiprocessing
            dfs = list(map_func(self._download_date,
                                (as_of for as_of in as_of_chunk if as_of not in self.settlement_df.index)))
            dfs = tuple(df for df in dfs if df is not None)  # Remove None entries
            if dfs:
                retval += len(dfs)
                # Persist Data to hdfs. This is the not-thread-safe part
                self.__settlement_df = self.settlement_df.append(dfs)
                self.dump()
        if retval:
            self.logger.info(f"Adjusting expirations for {self.__class__.__name__} {self.name()}")
            self.roll_expiration()
        return retval

    def dump(self) -> bool:
        """Saves settlement_df to database"""
        self.__settlement_df = self.__settlement_df.sort_index()
        # write to database
        retval = self._db_client_write.write_df(self.database, self.name(), self.__settlement_df)
        if not retval:
            self.logger.warning("Could not dump data")
        return retval

    def load(self):
        """Loads settlement_df from database"""
        if self.date_last_data_ts() is None:
            self.__settlement_df = pd.DataFrame(columns=pd.MultiIndex.from_arrays([[]] * len(df_index_columns),
                                                                                  names=df_index_columns))
        else:
            # Reads EVERYTHING in memory converted to float64!!!
            self.__settlement_df = self._db_client_write.read(self.database, self.name(), self.min_date()). \
                astype(np.float64)

    def roll_expiration(self, roll_offset=0) -> None:
        """
        Rolls product after expiration date
        second column is offset=2. After expiration, first column should have a nan value
        :param roll_offset: number of days before expiration to roll the contract
        :return: None
        """

        def column_idx(index, names, **kwargs):
            idx = tuple(kwargs.get(name, idx) for name, idx in zip(names, index))
            return idx

        # remove adj_close, ignoring non-existing columns
        self.__settlement_df = self.__settlement_df.drop(TypeColumn.adj_close.value, level="type", axis=1,
                                                         errors="ignore")
        # Force ordering
        self.__settlement_df = self.__settlement_df.sort_index()
        df_rolls = list()
        for _, group in self.settlement_df.groupby(["market", "commodity", "area", "product"], axis=1):
            # Just type=close in the group (ignoring any other type)
            group_close = group.xs(TypeColumn.close.value, level="type", axis=1, drop_level=False)
            if group_close.empty:
                continue
            index = group.columns[0]
            self.logger.info(f"Processing rolling of {index[:-1]}")
            product = group_close.columns.get_level_values("product").unique()[0]
            # Take offsets from the last row of available data
            last_available_row = np.argwhere(~group_close.isna().all(axis=1).values).max()
            max_offset = group_close.iloc[
                last_available_row,
                np.argwhere(~group_close.iloc[last_available_row, :].isna().values).flatten()  # not null columns
            ].xs(TypeColumn.close.value, level='type').index.get_level_values('offset').max()
            for offset in range(1, max_offset):
                df_prod_0 = group_close.loc[:,
                            column_idx(index, df_index_columns, offset=offset, type=TypeColumn.close.value)]
                df_prod_1 = group_close.loc[:,
                            column_idx(index, df_index_columns, offset=offset + 1, type=TypeColumn.close.value)]
                expirations = np.argwhere(np.diff(product_to_date(df_prod_0.index, product)) != 0).flatten()

                # df_prod_1 should not have nans, so fill them
                roll_values = roll(df_prod_0.values, df_prod_1.fillna(method="ffill").values, expirations, roll_offset)

                df_roll = pd.Series(roll_values, index=df_prod_0.index,
                                    name=column_idx(index, df_index_columns, offset=offset,
                                                    type=TypeColumn.adj_close.value))
                df_rolls.append(df_roll)

        # Append all rollings at the same time to avoid performance warning due to heavy fragmentation
        self.__settlement_df = pd.concat([self.settlement_df, *df_rolls], axis=1)
        self.dump()
        return None

    @abc.abstractmethod
    def _download_date(self, as_of: pd.Timestamp) -> pd.DataFrame:
        pass

    def as_of_str(self, as_of):
        """Formats a date to str using self.date_format"""
        if isinstance(as_of, str):
            return as_of
        else:
            return as_of.strftime(self.date_format)

    def http_get(self, url: str, params=None):
        """
        Performs a http get. Tries to perform it with validation and retries without validation on case of error
        :param url: the url to get
        :param params: (optional) the parameters of the url
        :return: a requests object
        """
        headers = self.headers or dict()
        if self.cookies:
            cookies = cookies2header(cookies=self.cookies)
            headers.update(cookies)
        req = self.http.request("get", url, headers=headers, fields=params)
        return req


def consecutive(data, stepsize=1):
    """Returns groups of consecutive values in data of size greater than stepsize"""
    return np.split(data, np.where(np.diff(data) != stepsize)[0] + 1)


def roll(price1: np.array, price2: np.array, expirations: np.array, roll_offset: int = 0) -> np.array:
    """
    Returns price1 rolled to price2 at given expiration dates
    :param price1: daily prices of the front contract (np array)
    :param price2: daily prices of the second front contract (that will be used to roll). Same size as price1. It
    SHOULD NOT CONTAIN NANs, they must be filled with .fillna(method="ffill").values (for pandas Series/DataFrame)
    :param expirations: index of expiry of the contract of price1
    :param roll_offset: number of days before expiry for doing contract rolling (by default 0, roll at expiry)
    :return: an array of same size as price1 with the price rolled: it means at expiry - roll_offset, the price1
    is turned to be price2 minus the gap between price1 and price2. Moreover, it takes into account that sometimes
    prices cease to be published before expiry (and they are nan) so expiry date is actually the date where last
    nan is found before official expiry index
    """
    price_roll = price1.copy()
    nan_indexes = np.argwhere(np.isnan(price1)).flatten()

    # If df_roll ends with a nan, add a fake expiration date to it, so it is forced to roll
    if nan_indexes.size != 0 and nan_indexes[-1] == len(price1 - 1):
        expirations = np.append(expirations, nan_indexes[-1])

    # Returns a reversed list of tuples with of start (including offset) and end indexes of nan consecutive groups
    nan_indexes_groups = list((max(0, v[0] - roll_offset - 1), v[-1])
                              for v in reversed(consecutive(nan_indexes)) if len(v))

    for expiry in reversed(expirations):
        idx_start = expiry - roll_offset
        idx_end = expiry
        for nan_group in nan_indexes_groups:
            if nan_group[0] <= expiry <= nan_group[-1]:
                idx_start, idx_end = nan_group
                nan_indexes_groups.remove(nan_group)
                break

        prc_roll_1 = price1[idx_start]
        prc_roll_2 = price2[idx_start]
        roll_value = prc_roll_2 - prc_roll_1
        # Fill with the following contract in expiration
        price_roll[idx_start:idx_end + 1] = price2[idx_start:idx_end + 1]
        # Do contract rolling
        price_roll[idx_start:] -= roll_value

    return price_roll
