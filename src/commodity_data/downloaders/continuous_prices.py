"""
Functions to calculate continuous product prices, by rolling prices at a certain offset before maturity
This will calculate the adj_close column of settlement_df
"""
import numpy as np
import pandas as pd

from commodity_data.common import logger
from commodity_data.downloaders.series_config import TypeColumn, df_index_columns


def consecutive(data, stepsize=1):
    """Returns groups of consecutive values in data of size greater than stepsize"""
    return np.split(data, np.where(np.diff(data) != stepsize)[0] + 1)


def column_idx(index, names=df_index_columns, **kwargs) -> tuple:
    """Calculates a column multiindex, based on a certain index but updating it to the given values"""
    idx = tuple(kwargs.get(name, idx) for name, idx in zip(names, index))
    return idx


# def product_to_date(obj, product: str):
#     """Transforms product as string (Y/M/Q/D) into functions call to datetime objects"""
#     # To convert standard frequencies into products
#     freqs = dict(Y="year",
#                  Q="quarter",
#                  M="month",
#                  D="day")
#     if product in freqs:
#         return getattr(obj, freqs[product])
#     else:
#         raise NotImplementedError(f"Product {product} is not implemented yet")


def calculate_continuous_prices(settlement_df: pd.DataFrame, valid_products: list = None,
                                continuous_price_type: str = TypeColumn.adj_close.value,
                                roll_offset: int = 0) -> pd.DataFrame:
    """
    Calculates adj_close columns for the given settlement_df (a pandas DataFrame with multiindex columns)
    :param settlement_df: a BaseDownloader.settlement_df pandas DataFrame
    :param valid_products: a list of valid products (e.g.: YMQWD) to calculate continuous_prices
    :param continuous_price_type: value for the level "type" of the column with the continuous proces
    :param roll_offset: number of business days for performing offset
    :return: a new pandas DataFrame with the adj_close calculated for the valid
    """

    # remove maturity, ignoring non-existing columns
    # settlement_df = settlement_df.drop(TypeColumn.maturity, level="type", axis=1, errors="ignore")

    valid_columns = settlement_df.columns.get_level_values('offset') > 0
    # Do not roll weeks
    # valid_columns = valid_columns & (settlement_df.columns.get_level_values('product') != "W")
    df_rolls = list()
    for _, group_t in settlement_df.loc[:, valid_columns].T.groupby(["market", "commodity", "area", "product"]):
        product = group_t.index.get_level_values("product")[0]
        index = group_t.index[0]
        if valid_products and product not in valid_products:
            logger.info(f"Skipping rolling of {index[:-1]}")
            continue
        logger.info(f"Processing rolling of {index[:-1]}")
        # As groupby with axis is deprecated, it has to be manually transposed back
        group = group_t.T
        # Drop rows with nans, as they have to be skipped
        group = group.dropna(how="all")
        # Just type=close in the group (ignoring any other type)
        group_close = group.xs(TypeColumn.close, level="type", axis=1, drop_level=False).astype(float)
        if group_close.empty:
            logger.info(f"Skipping {index[:-1]}: no data available")
            continue
        group_maturity = group.xs(TypeColumn.maturity, level="type", axis=1, drop_level=False).astype("datetime64[ns]")
        # Take offsets from the last row of available data
        max_offset = group_close.iloc[-1,
        np.argwhere(~group_close.iloc[-1].isna()).flatten()  # not null columns
        ].index.get_level_values('offset').max()
        for offset in range(1, int(max_offset)):
            # df_prod_0 = group_close.loc[:,
            #             column_idx(index, offset=offset, type=TypeColumn.close.value)]
            # df_prod_1 = group_close.loc[:,
            #             column_idx(index, offset=offset + 1, type=TypeColumn.close.value)]
            # expirations = np.argwhere(np.diff(product_to_date(df_prod_0.index, product)) != 0).flatten()
            df_prod_0 = group_close.xs(offset, level="offset", axis=1, drop_level=False)
            df_prod_1 = group_close.xs(offset + 1, level="offset", axis=1, drop_level=False)
            # use change in product maturities to calculate expirations
            expirations = np.argwhere(group_maturity.xs(offset, level="offset", axis=1).iloc[:, 0].astype(
                "datetime64[ns]").bfill().diff().dt.days > 0).flatten()
            # df_prod_1 should not have nans, so fill them
            roll_values = roll(df_prod_0.values.flatten(), df_prod_1.ffill().values.flatten(), expirations, roll_offset)
            df_roll = pd.Series(roll_values.flatten(), index=df_prod_0.index,
                                name=column_idx(index, offset=offset, type=continuous_price_type))
            df_rolls.append(df_roll)
    ns = settlement_df.columns.names
    settlement_df = pd.concat([settlement_df, *df_rolls], axis=1)
    settlement_df.columns.names = ns
    return settlement_df


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
        if np.isnan(prc_roll_2):
            # Prices cannot be rolled, as there is no price2. Set prices as nan and exit
            price_roll[:idx_start] = np.nan
            break
        roll_value = prc_roll_2 - prc_roll_1
        # Fill with the following contract in expiration
        price_roll[idx_start:idx_end + 1] = price2[idx_start:idx_end + 1]
        # Do contract rolling
        price_roll[idx_start:] -= roll_value

    return price_roll
