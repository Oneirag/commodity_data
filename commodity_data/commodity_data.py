import abc
import os
from commodity_data import http, DATA_DIR, logger
import numpy as np
import pandas as pd
import holidays
from commodity_data.series_config import df_data_columns, df_index_columns
import multiprocessing


def product_to_date(obj, product: str):
    """Transforms product as string (Y/M/Q/D) into functions call to datetime objects"""
    freqs = dict(Y="year",
                 Q="quarter",
                 M="month",
                 D="day")
    return getattr(obj, freqs[product])


class CommodityDownloader:

    def __init__(self, data_dir=DATA_DIR):
        """
        By default files are stored in ~/.ongpi/commodity_data. To make it work in google colab, this
        parameter can be changed in the constructor
        :param data_dir: a full path of folder where files will be downloaded. It is passed to os.path.expanduser
        """
        self.http = http
        self.logger = logger
        self.DATA_DIR = data_dir
        self.date_format = "%Y-%m-%d"
        # Headers for http gets. In case of a log in child classes should define a method for updating it
        self.headers = None

        # Filenames for persistent storage
        self.settlement_filename = os.path.join(self.DATA_DIR, self.file_name() + ".pkl")
        if os.path.isfile(self.settlement_filename):
            self.settlement_df = pd.read_pickle(self.settlement_filename)
        else:
            self.settlement_df = pd.DataFrame(columns=pd.MultiIndex.from_arrays([[]] * len(df_index_columns),
                                                                                names=df_index_columns))
        pass

    @abc.abstractmethod
    def min_date(self):
        """Returns minimum date for downloading"""
        return

    @abc.abstractmethod
    def file_name(self):
        """Returns file_name without extension"""
        return

    def settle_xs(self, **filter_):
        """Applies a xs to self.settlement_df with key as values and levels as keys of filter"""
        return self.settlement_df.xs(key=list(filter_.values()), level=list(filter_.keys()), axis=1, drop_level=False)

    def download(self, start_date: pd.Timestamp = None, end_date: pd.Timestamp = None) -> int:
        """
        Downloads and stores data from a start date to a end date
        :param start_date:
        :param end_date:
        :return: the number of downloaded days
        """
        retval = 0
        start_date = pd.Timestamp(start_date or self.min_date())
        end_date = pd.Timestamp(end_date or pd.Timestamp.today().normalize())
        ecb_hols = holidays.EuropeanCentralBank(years=range(start_date.year, end_date.year + 1))
        as_of_dates = pd.bdate_range(start_date, end_date, holidays=ecb_hols, freq="C")
        pool = multiprocessing.Pool(4)
        # Chunked in months (20 Business days aprox)
        for as_of_chunk in np.array_split(as_of_dates, 20):
            dfs = list(pool.map(self._download_date,
                                (as_of for as_of in as_of_chunk if as_of not in self.settlement_df.index)))
            dfs = tuple(df for df in dfs if df is not None)     # Remove None entries
            if dfs:
                retval += len(dfs)
                # Persist Data to hdfs. This is the not-thread-safe part
                self.settlement_df = self.settlement_df.append(dfs)
                self.dump()
        if retval or True:
            pass
            self.roll_expiration()
        return retval

    def dump(self):
        """Saves settlement_df to disk"""
        self.settlement_df = self.settlement_df.sort_index()
        self.settlement_df.to_pickle(self.settlement_filename)

    def roll_expiration(self, roll_offset=0) -> None:
        """
        Rolls product after expiration date
        :param df_prod: Dataframe with the series of the product ordered by offset: first column is offset=0 and
        second column is offset=2. After expiration, first column should have a nan value
        :param roll_offset: number of days before expiration to roll the contract
        :param product: product for computing "natural" expiration of the product. It can be D, M, or Y
        :return: None
        """

        def consecutive(data, stepsize=1):
            return np.split(data, np.where(np.diff(data) != stepsize)[0] + 1)

        def column_idx(index, names, **kwargs):
            idx = tuple(kwargs.get(name, idx) for name, idx in zip(names, index))
            return idx

        for _, group in self.settlement_df.groupby(["market", "commodity", "area", "product"], axis=1):
            # df_prod = group.xs("close", level="type", axis=1, drop_level=False)
            index = group.columns[0]
            logger.debug(f"Processing rolling of {index[:-1]}")
            product = group.columns.get_level_values("product").unique()[0]
            offsets = group.columns.get_level_values("offset")
            for offset in offsets[:-1]:
                df_prod_0 = group.loc[:, column_idx(index, df_index_columns, offset=offset, type="close")]
                df_prod_1 = group.loc[:, column_idx(index, df_index_columns, offset=offset + 1, type="close")]
                self.settlement_df.loc[:, column_idx(index, df_index_columns, offset=offset,
                                                     type="adj_close")] = df_prod_0
                df_roll = self.settlement_df.loc[:, column_idx(index, df_index_columns, offset=offset,
                                                               type="adj_close")]

                nan_indexes = np.argwhere(np.isnan(df_prod_0.iloc[:]).values).flatten()
                expirations = np.argwhere(np.diff(product_to_date(df_prod_0.index, product)) != 0).flatten()

                nan_indexes_groups = consecutive(nan_indexes)
                for expiry in reversed(expirations):
                    idx_start = expiry
                    idx_end = expiry + 1
                    for nan_group in nan_indexes_groups:
                        if len(nan_group) > 0:
                            group_start = nan_group[0]
                            group_end = nan_group[-1]
                            if expiry == group_end:
                                idx_start = min(idx_start, group_start - 1)
                                break
                    if idx_start == -1:
                        continue
                    idx_roll = idx_start - roll_offset
                    roll_value = df_prod_1.iloc[idx_roll] - df_prod_0.iloc[idx_roll]
                    # Fill with the following contract in expiration
                    df_roll.iloc[idx_start:idx_end] = df_prod_1.iloc[idx_start:idx_end]
                    # Do contract rolling
                    df_roll.iloc[idx_start:] -= roll_value
        self.dump()
        return None

    @abc.abstractmethod
    def _download_date(self, as_of: pd.DataFrame) -> dict:
        return None

    def as_of_str(self, as_of):
        """Formats a date to str using self.date_format"""
        if isinstance(as_of, str):
            return as_of
        else:
            return as_of.strftime(self.date_format)

    def http_get(self, url: str):
        """
        Performs an http get. Tries to perform it with validation and retries without validation on case of erro
        :param url: the url to get
        :return: a requests object
        """
        try:
            req = self.http.get(url, headers=self.headers)
        except Exception as e:
            req = self.http.get(url, headers=self.headers, verify=False)
        return req
