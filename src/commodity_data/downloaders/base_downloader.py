import abc
import multiprocessing.pool
import time

import holidays
import marshmallow_dataclass
import numpy as np
import ong_tsdb.exceptions
import pandas as pd
import pyotp
from ong_tsdb.client import OngTsdbClient
from ong_utils import is_debugging, cookies2header

from commodity_data import config
from commodity_data import http, logger, get_password
from commodity_data.downloaders.default_config import default_config
from commodity_data.series_config import df_index_columns, TypeColumn

pd.options.mode.chained_assignment = 'raise'  # Raises SettingWithCopyWarning error instead of just warning


class StoreDataException(Exception):
    """Exception raised when dump failed"""
    pass


class FilterKeyNotFoundException(Exception):
    """Exception raised when trying to filter data in settle_xs but data is not available for this downloader"""
    pass


# To convert standard frequencies into products
freqs = dict(Y="year",
             Q="quarter",
             M="month",
             D="day")


class GoogleAuth(pyotp.TOTP):
    last_otp = ""

    def now(self) -> str:
        """Returns a TOTP code, preventing reuse (waits for a new one if needed)"""
        otp = super().now()
        if otp == GoogleAuth.last_otp:
            logger.info("Waiting for new MFA code...")
            while super().now() == GoogleAuth.last_otp:
                time.sleep(1)
            logger.info("New MFA code generated")
        otp = super().now()
        GoogleAuth.last_otp = otp
        return otp


def product_to_date(obj, product: str):
    """Transforms product as string (Y/M/Q/D) into functions call to datetime objects"""

    return getattr(obj, freqs[product])


def combine_config(default_config: list, config: list, parser, use_default: bool = True) -> list:
    """
    Returns a list of parsed values from a combination of default_config and config, depending on parameters
    :param default_config: required, the default configuration (a list of dicts)
    :param config: optional list of dicts. If null, default_config will be used.If not null, behaviour depends
    on the values of replace param
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


class HttpGet:
    """Class that adds http_get functionality for downloading and connecting"""

    def __init__(self):
        self.http = http
        self.headers = None
        self.cookies = None

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
        if req.status >= 399:
            raise ConnectionError(f"Could not connect to {url}. Received status {req.status}: {req.reason}")
        return req


class _OngTsdbClientManager:
    __client = None
    __otp = None
    __write_token = config("write_token")
    __admin_token = config("admin_token")
    __server_url = config("url")
    logger = logger

    def __init__(self, name: str, ):
        self.name = name

    @classmethod
    def proxy_auth_dict(cls, name: str) -> dict | None:
        """Returns proxy auth dict, including MFA Code"""
        if config("service_name_google_auth", None) is not None and cls.__otp is None:
            cls.__otp = GoogleAuth(get_password("service_name_google_auth", "proxy_username"))
        if cls.__otp is None:
            return None
        cls.logger.info(f"Getting MFA code for {name}")
        mfa_code = cls.__otp.now()
        proxy_auth_dict = dict(username=config("proxy_username"),
                               password=get_password("service_name_proxy", "proxy_username"),
                               mfa_code=mfa_code)
        return proxy_auth_dict

    @classmethod
    def __create_admin_client(cls, name: str):
        if cls.__client is None:
            cls.__client = OngTsdbClient(cls.__server_url, cls.__admin_token, retry_connect=1,
                                         retry_total=1, proxy_auth_body=cls.proxy_auth_dict(name))

    @property
    def admin_client(self) -> OngTsdbClient:
        if self.__client is None:
            try:
                self.__create_admin_client(self.name)
            except ong_tsdb.exceptions.ProxyNotAuthorizedException:
                self.logger.info("Could not get proxy authorization, retrying")
                # Try again
                self.__create_admin_client(self.name)
        self.__client.update_token(self.__admin_token)
        return self.__client

    @property
    def write_client(self) -> OngTsdbClient:
        client = self.admin_client
        client.update_token(config("write_token"))
        return client


class BaseDownloader(HttpGet):
    period = "1D"
    database = "commodity_data"

    def __init__(self, name: str, config_name: str, class_schema, default_config_field: str,
                 roll_expirations: bool = True):
        """
        Initializes the Downloader, creating clients using configuration. It needs url, host, admin_token, write_token
        and read_token keys
        :param name: Name of the sensor that will be created to store data
        :param config_name: default config name in the config file
        :param class_schema: class used to parse the configuration
        :param default_config_field: name for default config field
        :param roll_expirations: True (default) to calculate adj_close by adjusting expirations, False to ignore them
        """
        super().__init__()
        self.__roll_expirations = roll_expirations
        self.__name = name
        self.date_format = "%Y-%m-%d"
        self.logger = logger
        self.__client = None
        self.first_use = False
        self.__settlement_df = None
        self.cache = None
        self.last_data_ts = None
        self.config = None
        self.create_config(config_name, class_schema, default_config_field)

    def delete_all_data(self, do_not_ask: bool = False) -> bool:
        """Deletes the whole remote database. Ask for confirmation. Returns True if deleted"""
        if do_not_ask or ("yes" == input(f"Type 'yes' if you are sure to delete all {self.name()} data: ")):
            if self.db_client_admin.delete_sensor(self.database, self.name()):
                self.logger.info(f"Deleted all market data for '{self.name()}' from database '{self.database}'")
                self.verify_database()
                return True
            else:
                self.logger.warning(f"Could not delete market data '{self.name()}' from database '{self.database}'")
                return False

    def create_config(self, config_field: str, class_schema, default_config_field: str):
        if not self.name() in default_config:
            raise ValueError(f"Could not find {self.name()} in default configuration")
        base_config = default_config[self.name()]
        cfg = config(config_field, dict())
        parser = marshmallow_dataclass.class_schema(class_schema)()
        self.config = combine_config(base_config, cfg, parser,
                                     use_default=config(default_config_field, True))

    @property
    def db_client_write(self) -> OngTsdbClient:
        if not self.__client:
            admin = self.db_client_admin  # Forces initialization of db is client write is used before client admin
        return self.__client.write_client

    @property
    def db_client_admin(self) -> OngTsdbClient:
        """Creates a new admin client. If it is the first time or force_reload=False, creates the client,
        the database and all the sensors"""

        if self.__client is None:
            self.__client = _OngTsdbClientManager(self.name())
            self.verify_database()
        return self.__client.admin_client

    def verify_database(self):
        """Verifies that the database exists, setting up it if no"""
        admin_client = self.__client.admin_client
        if not admin_client.exist_db(self.database):
            admin_client.create_db(self.database)
        if not admin_client.exist_sensor(self.database, self.name()):
            admin_client.create_sensor(self.database, self.name(), self.period, [],
                                       config("read_token"), config("write_token"),
                                       level_names=df_index_columns)
        else:
            if not admin_client.get_metadata(self.database, self.name()):
                admin_client.set_level_names(self.database, self.name(), df_index_columns)
        admin_client.config_reload()  # Forces config reload in case external changes found
        self.last_data_ts = self.date_last_data_ts()
        self.logger.info(f"Data for {self.name()} available up to {self.last_data_ts}")

    @property
    def settlement_df(self):
        if self.__settlement_df is None:
            self.load()
        return self.__settlement_df

    def date_last_data_ts(self):
        """Returns last date (for any data in current database)"""
        # Admin client must be used, as it fails if sensor does not exist so there are no permissions for getting date
        last_date = self.db_client_write.get_lastdate(self.database, self.name())
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

    def settle_xs(self, allow_zero_prices: bool = True, **filter_):
        """Applies a xs to self.settlement_df with key as values and levels as keys of filter"""
        maturity_value = None if "maturity" not in filter_ else pd.Timestamp(filter_.pop('maturity')).timestamp()
        if all(col in filter_ for col in ("maturity", "offset")):
            raise ValueError("Cannot filter by offset and maturity at the same time")
        if maturity_value:
            filter_df = self.settlement_df[
                self.settlement_df.xs("maturity", level="type", axis=1) == maturity_value].dropna(axis=1, how="all")
        else:
            filter_df = self.settlement_df
        try:
            retval = filter_df.xs(key=tuple(filter_.values()), level=tuple(filter_.keys()), axis=1,
                                  drop_level=False)
            if maturity_value:
                names = list(retval.columns.names)
                names.remove("offset")
                retval = retval.T.groupby(level=names).sum().T
            else:
                # Remove maturity if not explicitly asked for it
                retval = retval.loc[:, retval.columns.get_level_values('type') != 'maturity']
            if not allow_zero_prices:
                retval[retval == 0] = None
            return retval
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
            raise FilterKeyNotFoundException(f"Key {failed_key} not found in level '{failed_level}' "
                                             f"with available values {values_failed_level}. "
                                             f"Key was found in level {level_failed_key}") from None

    def pivot_table(self, df: pd.DataFrame, value_columns: list) -> pd.DataFrame:
        levels = df_index_columns[:-1]  # Ignores "type"
        type_level = df_index_columns[-1]

        not_found_columns = set(list(["as_of", *levels, *value_columns])) - set(df.columns)
        if not_found_columns:
            error_msg = f"Could not find {not_found_columns} in provided dataframe"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        # Use pd.pivot_table to reshape the DataFrame
        df_pivot = pd.pivot_table(df, index='as_of', columns=levels, values=value_columns)
        # Fix level order and names
        level_idx = list(range(len(df_index_columns)))
        # Put type the last
        level_idx = level_idx[1:] + level_idx[:1]
        df_pivot.columns = df_pivot.columns.reorder_levels(level_idx).set_names(df_index_columns)
        return df_pivot

    def get_holidays(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> dict:
        """As a default, return the holidays of the ECB, which are target holidays"""
        return holidays.EuropeanCentralBank(years=range(start_date.year, end_date.year + 1))

    def prepare_cache(self, start_date: pd.Timestamp, end_date: pd.Timestamp, force_download: bool):
        """
        This function will be called before daily processing of data to prepare a cache of data.
        Ignore it if no cache will be used, define in child classes to use cache
        :param start_date:  start date for downloading data
        :param end_date: end date for downloading data
        :param force_download: True to force download
        :return: None
        """
        pass

    def download(self, start_date: pd.Timestamp = None, end_date: pd.Timestamp = None,
                 force_download: bool = False) -> int:
        """
        Downloads and stores data from a start date to an end date
        :param start_date:
        :param end_date:
        :param force_download: True to force download again data. Defaults to False (avoid downloading again)
        :return: the number of downloaded days
        """
        retval = 0
        start_date = pd.Timestamp(start_date or self.min_date())
        end_date = pd.Timestamp(end_date or pd.Timestamp.today().normalize())
        # If there is data already stored, unless force_download avoid downloading data older than 10 years
        if self.last_data_ts and not force_download:
            start_date = max(start_date, self.last_data_ts - pd.offsets.YearBegin(10))
        self.prepare_cache(start_date, end_date, force_download)
        ecb_hols = self.get_holidays(start_date, end_date)
        as_of_dates = pd.bdate_range(start_date, end_date, holidays=ecb_hols, freq="C")
        # Chunked in months (20 Business days aprox)
        for as_of_chunk in np.array_split(as_of_dates, 20):
            # In case of debugging, don't use multiprocessing
            # map_func = map if self.cache is not None or is_debugging() else multiprocessing.Pool(4).map
            map_func = map if self.cache is not None or is_debugging() else multiprocessing.pool.ThreadPool(4).map
            # map_func = map      # No multiprocessing
            dfs = list(map_func(self._download_date,
                                (as_of for as_of in as_of_chunk
                                 if force_download or as_of not in self.settlement_df.index)))
            dfs = tuple(df for df in dfs if df is not None)  # Remove None entries
            if dfs:
                retval += len(dfs)
                # Persist Data to hdfs. This is the not-thread-safe part
                new_data = pd.concat(dfs)
                if self.__settlement_df is not None:
                    self.__settlement_df.update(new_data)
                    self.__settlement_df.sort_index()
                else:
                    self.__settlement_df = new_data
                self.dump(new_data)
        if retval and self.__roll_expirations:
            self.logger.info(f"Adjusting expirations for {self.__class__.__name__} {self.name()}")
            self.roll_expiration()
        return retval

    def dump(self, df: pd.DataFrame = None) -> bool:
        """Saves give df to database. If None, self.__settlement_df will be saved"""
        if df is None:
            df = self.__settlement_df.sort_index()
        if df.empty:
            return True
        # write to database
        # Be careful with maturity: it cannot be saved as date and has to be converted to timestamp
        retval = self.db_client_write.write_df(self.database, self.name(), df)
        if not retval:
            self.logger.error("Could not dump data")
            self.logger.info("Try to update proxy password using set_proxy_user_password() of __init__")
            raise StoreDataException("Could not dump data")
        return retval

    def load(self):
        """Loads settlement_df from database"""
        if self.date_last_data_ts() is None:
            self.__settlement_df = pd.DataFrame(columns=pd.MultiIndex.from_arrays([[]] * len(df_index_columns),
                                                                                  names=df_index_columns))
        else:
            # Reads EVERYTHING in memory converted to float64!!!
            self.__settlement_df = self.db_client_write.read(self.database, self.name(), self.min_date()). \
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
        # for _, group in self.settlement_df.groupby(["market", "commodity", "area", "product"], axis=1):
        for _, group in self.settlement_df.T.groupby(["market", "commodity", "area", "product"]):
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
            for offset in range(1, int(max_offset)):
                df_prod_0 = group_close.loc[:,
                            column_idx(index, df_index_columns, offset=offset, type=TypeColumn.close.value)]
                df_prod_1 = group_close.loc[:,
                            column_idx(index, df_index_columns, offset=offset + 1, type=TypeColumn.close.value)]
                expirations = np.argwhere(np.diff(product_to_date(df_prod_0.index, product)) != 0).flatten()

                # df_prod_1 should not have nans, so fill them
                roll_values = roll(df_prod_0.values, df_prod_1.ffill().values, expirations, roll_offset)
                df_roll = pd.Series(roll_values, index=df_prod_0.index,
                                    name=column_idx(index, df_index_columns, offset=offset,
                                                    type=TypeColumn.adj_close.value))
                df_rolls.append(df_roll)

        # Append all rollings at the same time to avoid performance warning due to heavy fragmentation
        ns = self.settlement_df.columns.names
        self.__settlement_df = pd.concat([self.settlement_df, *df_rolls], axis=1)
        self.settlement_df.columns.names = ns
        self.dump(self.__settlement_df)  # Full dump due to rolling adjustments
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
