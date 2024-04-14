import abc
import logging
import multiprocessing.pool
import time

import holidays
import marshmallow_dataclass
import numpy as np
import ong_tsdb.exceptions
import pandas as pd
import pyotp
from ong_tsdb.client import OngTsdbClient
from ong_utils import is_debugging, cookies2header, OngTimer

from commodity_data.downloaders.continuous_prices import calculate_continuous_prices
from commodity_data.downloaders.default_config import default_config
from commodity_data.downloaders.series_config import df_index_columns, TypeColumn
from commodity_data.globals import config, logger, http, get_password

pd.options.mode.chained_assignment = 'raise'  # Raises SettingWithCopyWarning error instead of just warning


def update_dataframe(old_df: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
    """Updates an old_df adding columns and rows of the new data dataframe"""
    if old_df is not None and not old_df.empty:
        # old_df.update(new_data)
        # old_df.loc[new_data.index, new_data.columns] = new_data
        retval = old_df.combine_first(new_data)
        retval.sort_index(inplace=True)
        return retval
    else:
        return new_data


class StoreDataException(Exception):
    """Exception raised when dump failed"""
    pass


class FilterKeyNotFoundException(Exception):
    """Exception raised when trying to filter data in settle_xs but data is not available for this downloader"""
    pass


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
        self.__download_config = self.create_config(config_name, class_schema, default_config_field)
        self.__download_config_filter = list()
        self.set_force_download_filter(None)  # Initialize, just in case

    @property
    def download_config(self):
        return self.__download_config

    def set_force_download_filter(self, config_filter: bool | dict | list | None):
        """Sets the config filter used for forcing downloading of just a specific commodity"""
        # If the filter is True, False or None reverts to the list all configs,
        # So when calling to download method all configs will be updated
        if any(config_filter is something for something in (True, False, None)):
            self.__download_config_filter = [cfg.download_cfg for cfg in self.__download_config]
            return
        if isinstance(config_filter, dict):
            config_filter = [config_filter]
        try:
            self.__download_config_filter = []
            for cdty_cfg in self.__download_config:
                for filter_cfg in config_filter:
                    if cdty_cfg.download_cfg.meets(filter_cfg):
                        self.__download_config_filter.append(cdty_cfg.download_cfg)
                        break
        except Exception as e:
            self.logger.error(f"Force Download filter has invalid fields: {e}")
            raise

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

    def create_config(self, config_field: str, class_schema, default_config_field: str) -> list:
        """Returns a list so it can be overridden in child classes"""
        if not self.name() in default_config:
            raise ValueError(f"Could not find {self.name()} in default configuration")
        base_config = default_config[self.name()]
        cfg = config(config_field, dict())
        parser = marshmallow_dataclass.class_schema(class_schema)()
        retval = combine_config(base_config, cfg, parser,
                                use_default=config(default_config_field, True))
        return retval

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
        self.date_last_data_ts()
        self.logger.info(f"Data for {self.name()} available up to {self.last_data_ts}")

    @property
    def settlement_df(self):
        if self.__settlement_df is None:
            self.load()
        return self.__settlement_df

    def date_last_data_ts(self):
        """Returns last date (for any data in current database)"""
        # Admin client must be used, as it fails if sensor does not exist so there are no permissions for getting date
        self.last_data_ts = self.db_client_write.get_lastdate(self.database, self.name())
        return self.last_data_ts

    @abc.abstractmethod
    def min_date(self):
        """Returns minimum date for downloading, that will be last data downloaded"""
        if self.last_data_ts is None:
            self.last_data_ts = self.date_last_data_ts()
        return self.last_data_ts

    def name(self) -> str:
        """Returns the name of the origin"""
        return self.__name

    def maturity2timestamp(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """Converts maturity values to timestamp"""
        return self.__maturity_to("timestamp", df)

    def maturity2datetime(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """Converts maturity values to datatime objects"""
        return self.__maturity_to("datetime", df)

    def __maturity_to(self, what: str, df: pd.DataFrame = None) -> pd.DataFrame:
        """Converts inplace maturity values to timestamp (what='timestamp') or to datetime (what='datetime')"""
        is_settle = df is None
        df = self.settlement_df if is_settle else df
        if df.empty:
            return df
        index_maturity = df.columns.get_level_values('type') == 'maturity'
        if not index_maturity.any():
            return df
        maturity = df.loc[:, index_maturity]
        if maturity.empty:
            return df

        def transform(value: pd.Timestamp | float) -> pd.Timestamp | float | None:
            if what == "datetime":
                # return pd.Timestamp.fromtimestamp(value).normalize()
                if not pd.isna(value) and value > 0:
                    return pd.Timestamp.fromtimestamp(value).normalize()
                return None
            elif what == "timestamp":
                if not pd.isna(value):  # and value > 0:
                    return value.timestamp()
                return None

        # Perform the transformation of values, inplace
        dtypes = {
            "datetime": 'datetime64[ns]',
            "timestamp": float
        }
        df_dtypes = df.dtypes
        df = df.astype("object")  # To avoid problems with conversions
        for col in df.columns[index_maturity]:
            # df.loc[:, col].apply(lambda x: x*1e9).astype(dtypes[what]) # from ts to datetime
            if df_dtypes[col] != dtypes[what]:
                df.loc[:, col] = df.loc[:, col].apply(transform)
                df_dtypes[col] = dtypes[what]
            # df = df.assign(**{col: df.loc[:, col].apply(transform)}) #.astype(dtypes[what]))
        # convert dtypes back
        df = df.astype(df_dtypes)

        if is_settle:
            self.__settlement_df = df
        return df

    def settle_xs(self, allow_zero_prices: bool = True, market=None, commodity=None, instrument=None, area=None,
                  product=None, offset=None, type=None, maturity=None):
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
        filter_ = {k: v for k, v in filter_.items() if v}
        maturity_value = None if "maturity" not in filter_ else pd.Timestamp(filter_.pop('maturity'))
        if all(col in filter_ for col in ("maturity", "offset")):
            raise ValueError("Cannot filter by offset and maturity at the same time")
        if maturity_value:
            filter_df = self.settlement_df[
                self.settlement_df.xs("maturity", level="type", axis=1) == maturity_value].dropna(axis=1, how="all")
        else:
            filter_df = self.settlement_df
        try:
            retval = filter_df
            for level, key in filter_.items():
                if isinstance(key, (list, tuple)):
                    # key = tuple(key)
                    retval = retval.loc[:, retval.columns.get_level_values(level).isin(key)]
                else:
                    retval = retval.xs(key=key, level=level, axis=1, drop_level=False)
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
        """Pivots a DataFrame to create Multiindex columns. Makes sure that the provided DataFrame
         has the required columns (those of df_index_columns plus "as_of" for the index plus the
         ones in value_columns). The columns of value_columns will be the ones that form the
         last level of the column multiindex, 'type'"""
        levels = df_index_columns[:-1]  # Ignores "type"
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

    def iter_download_config(self):
        """Returns an interator of configurations"""
        for config in self.__download_config:
            if config.download_cfg in self.__download_config_filter:
                yield config

    def download(self, start_date: pd.Timestamp = None, end_date: pd.Timestamp = None,
                 force_download: bool = False) -> int:
        """
        Downloads and stores data from a start date to an end date
        :param start_date:
        :param end_date:
        :param force_download: True to force download again data. Defaults to False (avoid downloading again).
        It can be a bool (to download again all data), or a dict or list of dictionaries that will be used
        as filter to download again just a specific set of commodities. Dict fields must be compatible with
        commodity_data.series_config.CommodityCfg dataclass. Example: force_download=dict(instrument="BL") will
        only download baseload products
        :return: the number of downloaded days
        """
        self.set_force_download_filter(force_download)
        retval = 0
        start_date = pd.Timestamp(start_date or self.min_date())
        end_date = pd.Timestamp(end_date or pd.Timestamp.today().normalize())
        # If there is data already stored, unless force_download avoid downloading data older than 10 years
        if self.date_last_data_ts() and not force_download:
            start_date = max(start_date, self.last_data_ts.tz_localize(start_date.tz) - pd.offsets.YearBegin(10))
        self.prepare_cache(start_date, end_date, force_download)
        ecb_hols = self.get_holidays(start_date, end_date)
        as_of_dates = pd.bdate_range(start_date, end_date, holidays=ecb_hols, freq="C")
        # Chunked in months (20 Business days aprox)
        for as_of_chunk in np.array_split(as_of_dates, 20):
            if as_of_chunk.empty:
                break  # No additional downloads
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
                new_data = self.maturity2datetime(pd.concat(dfs))
                self.__settlement_df = update_dataframe(self.__settlement_df, new_data)
                self.dump(new_data)
        if retval and self.__roll_expirations:
            self.logger.info(f"Adjusting expirations for {self.__class__.__name__} {self.name()}")
            self.roll_expiration()
        self.set_force_download_filter(None)
        self.verify_database()  # Metadata might have been deleted...
        return retval

    def dump(self, df: pd.DataFrame = None) -> bool:
        """Saves give df to database. If None, self.__settlement_df will be saved"""
        if df is None:
            df = self.__settlement_df
        if df.empty:
            return True
        # write to database
        # Be careful with maturity: it cannot be saved as date and has to be converted to timestamp
        df = self.maturity2timestamp(df)
        msg = f"Writing data of size {df.shape} to database"
        self.logger.info(msg)
        with OngTimer(logger=self.logger, msg=msg, log_level=logging.INFO):
            retval = self.db_client_write.write_df(self.database, self.name(), df, fill_value=np.nan)
        if not retval:
            self.logger.error("Could not dump data")
            self.logger.info("Try to update proxy password using set_proxy_user_password() of commodity_data.common.py")
            raise StoreDataException("Could not dump data")
        return retval

    def delete_dates(self, start_date: pd.Timestamp, end_date: pd.Timestamp, reload: bool = True):
        """Deletes data from a specific date, by writing NaNs to all its values, including adjusted closes"""
        all_data = self.settlement_df
        all_data[start_date:end_date] = None
        settle = all_data[start_date:end_date]
        dump_ok = self.dump(settle)
        if dump_ok and reload:
            self.load()

    def load(self):
        """Loads settlement_df from database"""
        if self.date_last_data_ts() is None:
            self.__settlement_df = pd.DataFrame(columns=pd.MultiIndex.from_arrays([[]] * len(df_index_columns),
                                                                                  names=df_index_columns))
        else:
            # Reads EVERYTHING in memory converted to float64!!!
            self.__settlement_df = self.db_client_write.read(self.database, self.name(), self.min_date()). \
                astype(np.float64)
            self.__settlement_df.sort_index(inplace=True)
            self.__settlement_df.sort_index(inplace=True, axis=1)
            self.maturity2datetime()

    def roll_expiration(self, roll_offset=0, valid_products: list = None, valid_commodities: list = None,
                        valid_areas: list = None) -> None:
        """
        Rolls product after expiration date
        second column is offset=2. After expiration, first column should have a nan value
        :param roll_offset: number of days before expiration to roll the contract
        :param valid_products: optional list of products, to roll just the products in that list
        :param valid_commodities: optional list of commodities, to roll just the commodities in that list
        :param valid_areas: optional list of areas, to roll just the areas in that list
        :return: None
        """
        # remove adj_close, ignoring non-existing columns
        settlement_df = self.settlement_df.drop(TypeColumn.adj_close, level="type", axis=1,
                                                errors="ignore")
        # remove maturity, ignoring non-existing columns
        # settlement_df = settlement_df.drop(TypeColumn.maturity, level="type", axis=1, errors="ignore")

        # Force ordering
        settlement_df = settlement_df.sort_index()

        settlement_df = calculate_continuous_prices(settlement_df, roll_offset=roll_offset,
                                                    valid_products=valid_products, valid_commodities=valid_commodities,
                                                    valid_areas=valid_areas)

        diff_columns = [c for c in settlement_df.columns if c not in self.settlement_df or
                        (settlement_df[c].fillna(0) != self.settlement_df[c].fillna(0)).any()]
        if diff_columns:
            # Update with the changes
            self.__settlement_df = settlement_df
            # Append all rollings at the same time to avoid performance warning due to heavy fragmentation

            self.dump(self.__settlement_df[diff_columns])  # Full dump due to rolling adjustments
        return None

    @abc.abstractmethod
    def _download_date(self, as_of: pd.Timestamp) -> pd.DataFrame:
        pass

    def as_of_str(self, as_of) -> str:
        """Formats a date to str using self.date_format"""
        if isinstance(as_of, str):
            return as_of
        else:
            return as_of.strftime(self.date_format)
