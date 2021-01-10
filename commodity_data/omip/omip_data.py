import os
from datetime import datetime

import holidays
import numpy as np
import pandas as pd

from commodity_data import logger
from commodity_data.commodity_data import CommodityDownloader
from commodity_data.omip import OmipConfig
from commodity_data.series_config import df_index_columns


class OmipDownloader(CommodityDownloader):

    def file_name(self):
        return "Omip"

    def __init__(self, data_dir=None):
        if data_dir:
            super().__init__(data_dir)
        else:
            super().__init__()
        # Calculate the absolute minimum date for download
        self.__min_date = min(cdty_cfg['start_t'] for cdty_cfg in OmipConfig.commodity_config.values())

    def min_date(self):
        return self.__min_date

    def _download_date(self, as_of: pd.DataFrame) -> dict:
        dfs = list()
        for cdty, cdty_config in OmipConfig.commodity_config.items():
            logger.info(f"Downloading {cdty} for date {self.as_of_str(as_of)}")
            df = self.__download_omip_data(self.as_of_str(as_of), **cdty_config['download_config'])
            if df is None or df.empty:
                continue        # Skip if empty or None
            for c in df_index_columns:
                if c not in df.columns:
                    if c in cdty.__dict__:
                        df[c] = getattr(cdty, c)
                    else:
                        df[c] = None
            df['market'] = self.file_name()
            df['type'] = "close"
            df = df.drop(columns=['maturity'])
            #df = df.set_index(df_index_columns)
            df = pd.pivot_table(df, values="close", index="as_of", columns=df_index_columns)
            #for c in df_data_columns:
            #    if c not in df.columns:
            #        df[c] = None
            dfs.append(df)
        if dfs:
            return pd.concat(dfs, axis=1)
        else:
            return None

    def __download_omip_data(self, as_of: str, instrument="FTB", product="EL", zone="ES") -> pd.DataFrame:
        """
        Downloads omip data for a certain date into a pandas DataFrame.
        If no data is found returns None
        Examples:
            - Download Spanish power baseload futures: download(as_of) or
                        download(as_of, instrument="FTB", zone="ES", product="EL")
            - Download German power baseload futures: download(as_of, instrument="FDB", zone="DE")
            - Download French power baseload futures: download(as_of, instrument="FFB", zone="FR")
            - Download Spanish gas futures: download(as_of, instrument="FGE", zone="ES", product="NG")

        :param as_of: settlement date of the prices
        :param instrument:
        :param product: "EL" (power) or "NG" (natural gas)
        :param zone: "ES", "PT", "FR", "DE"
        :return: None if no data was found or a pandas DataFrame that has the following columns
            - "product" ("Y", "M", "Q", "D")
            - "maturity" (start date of delivery)
            - "Reference Prices": settlement prices (float)
            - "as_of"
        """
        url = f"https://www.omip.pt/en/dados-mercado?date={as_of}" \
              f"&product={product}&zone={zone}&instrument={instrument}"
        req = self.http_get(url)
        as_of_ts = pd.Timestamp(as_of)
        try:
            tables = (t.dropna(axis=0, how="all") for t in pd.read_html(req.content, decimal="."))
        except ValueError:
            # No tables found in website
            return None
        valid_tables = list()
        for table in tables:
            table = table.drop(index=0)
            table.set_index(table.columns[0], inplace=True)
            product = table.index[0].split(instrument)[1].strip()
            if product[0] in "DMYQ":
                table.index = pd.MultiIndex.from_tuples(
                    list(parse_omip_product_maturity_offset(idx.split(instrument)[-1].strip(), as_of_ts)
                         for idx in table.index),
                    names=["product", "maturity", "offset"])
                table = table.drop(columns=(n for n in table.columns if n != "Reference prices"))
                table["close"] = pd.to_numeric(table["Reference prices"], errors='coerce')
                table = table.dropna(axis=0, how="any")
                if table.empty:
                    logger.debug(f"No valid data for {as_of}, returning None")
                    return None     # No valid tables found
                table = table.reset_index()
                table = table.drop(columns=["Reference prices"])
                valid_tables.append(table)

        df = pd.concat(valid_tables)
        df['as_of'] = pd.Timestamp(as_of)
        return df


def parse_omip_product_maturity_offset(omip_product: str, as_of: pd.Timestamp) -> tuple:
    """Gets a 3 element tuple of :
        Item 0: product name ("Y" for year, "Q" for quarter, "M" for month and "D" for year)
        Item 1: its maturity (start date of delivery) from an Omip product description
        Item 2: its offset from the start date
    If product can not be parsed returns None, None, None"""
    if omip_product.startswith("M"):
        date_str = omip_product[2:]
        maturity = pd.Timestamp(datetime.strptime(date_str, "%b-%y"))
        offset = (maturity.year - as_of.year) * 12 + (maturity.month - as_of.month)
    elif omip_product.startswith("D"):
        date_str = omip_product[2:]
        maturity = pd.Timestamp(datetime.strptime(date_str[2:], "%d%b-%y"))
        offset = (maturity - as_of).days
    elif omip_product.startswith("Y"):
        year = int(omip_product[-2:])
        maturity = pd.Timestamp(year=(2000 + year), month=1, day=1)
        offset = maturity.year - as_of.year
    elif omip_product.startswith("Q"):
        date_str = omip_product
        quarter = int(date_str[1])
        year = int(date_str[3:])
        maturity = pd.Timestamp(year=(2000 + year), month=quarter * 3 - 2, day=1)
        offset = (maturity.year - as_of.year) * 4 + (maturity.quarter - as_of.quarter)
    else:
        return None, None, None

    return omip_product[0], maturity, offset


def download_data(commodity_name: str, start_year: int = 2014, start_month: int = 1,
                  start_day: int = 1) -> pd.DataFrame:
    """Downloads data starting in the informed data for a certain commodity.
    Data is stored in DATA_INPUT_DIR/omip. Returns a DataFrame with the updated data"""
    start_date = pd.Timestamp(start_year, start_month, start_day)

    if commodity_name not in OmipConfig.commodity_config:
        raise ValueError(f"Commodity {commodity_name} not understood")

    cdty_config = OmipConfig.commodity_config[commodity_name]
    download_config = cdty_config['download_config']
    omip_start_t = max(start_date, cdty_config['start_t'])
    omip_end_t = pd.Timestamp.today() - pd.offsets.BDay(1)
    ecb_hols = holidays.EuropeanCentralBank(years=range(omip_start_t.year, omip_end_t.year + 1))
    omip_dates = pd.bdate_range(omip_start_t, omip_end_t, holidays=ecb_hols, freq="C")
    if os.path.exists(get_filename(commodity_name)):
        df_buffer = pd.read_pickle(get_filename(commodity_name))
        logger.info(f"Data read from {get_filename(commodity_name)}")
    else:
        df_buffer = pd.DataFrame()
        print(f"No pickle found. Processing from scratch")

    for as_of_date in omip_dates:
        if as_of_date in df_buffer.index:
            row = df_buffer.loc[as_of_date]
            if not row.empty and not row.isna().all().all():
                continue
        as_of = str(as_of_date)[:10]
        logger.info(f"Downloading {as_of} for {commodity_name}")
        df = download_omip_data(as_of, **download_config)
        if df is not None and not df.empty:
            df.as_of = pd.to_datetime(df.as_of)
            df.maturity = pd.to_datetime(df.maturity)
            df.set_index(df.as_of, inplace=True)
            df_buffer = df_buffer.append(df, sort=True)
            # df_buffer.to_csv(get_filename(commodity_name, ext="csv"))
            df_buffer.to_pickle(get_filename(commodity_name, ext="pkl"))
        else:
            logger.info(f"No data found for {as_of}")
    return df_buffer


def invoke_frecuency(obj, frecuency="Y"):
    """Transforms frequencies as string into functions call to datetime objects"""
    freqs = dict(Y="year",
                 Q="quarter",
                 M="month",
                 D="day")
    return getattr(obj, freqs[frecuency])


def roll_expiration(df_prod, roll_offset=0, product: str = "Y") -> None:
    """
    Rolls product after expiration date
    :param df_prod: Dataframe with the series of the product ordered by offset: first column is offset=0 and
    second column is offset=2. After expiration, first column should have a nan value
    :param roll_offset: number of days before expiration to roll the contract
    :param product: product for computing "natural" expiration of the product. It can be D, M, or Y
    :return: None
    """

    nan_indexes = np.argwhere(np.isnan(df_prod.iloc[:, 0]).values).flatten()
    expirations = np.argwhere(np.diff(invoke_frecuency(df_prod.index, product)) != 0).flatten()

    def consecutive(data, stepsize=1):
        return np.split(data, np.where(np.diff(data) != stepsize)[0] + 1)

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
        idx_roll = idx_start - roll_offset
        roll_value = df_prod.iloc[idx_roll, 1] - df_prod.iloc[idx_roll, 0]
        # Fill with the following contract in expiration
        df_prod.iloc[idx_start:idx_end, 0] = df_prod.iloc[idx_start:idx_end, 1]
        # Do contract rolling
        df_prod.iloc[idx_start:, 0] -= roll_value
    return df_prod


def maturity2offset(x):
    """For a specific row in a dataframe, uses dat_maturity, cod_product and dat_pricedate to calculate
    the difference between today and the maturity in units of cod product."""
    maturity = x.maturity
    product = x.product
    as_of = x.as_of
    if product == "D":
        offset = (maturity - as_of).days
    else:
        offset = invoke_frecuency(maturity, product) - invoke_frecuency(as_of, product)
        if product != "Y":  # adjust year differences for quarters, months and days
            delta_years = maturity.year - as_of.year
            factor_years = {"M": 12, "Q": 4}
            offset += delta_years * factor_years[product]
    return offset


def get_continuous_products(df, show_plots=False) -> pd.DataFrame:
    """Converts products with different maturities (e.g. product=Y maturity=2020-01-01) into continuous
    products (that roll on expiration. E.g. Y+1) """

    df.reset_index(inplace=True, drop=(df.index.name in df.columns))

    df.rename(columns={"maturity": "dat_maturity",
                       "as_of": "dat_pricedate",
                       "Reference prices": "num_price",
                       "product": "cod_product"}, inplace=True)

    df.dat_pricedate = pd.to_datetime(df.dat_pricedate)
    df.dat_maturity = pd.to_datetime(df.dat_maturity)
    df['num_offset'] = df.apply(maturity2offset, axis=1, raw=True)

    df_pivot = pd.pivot_table(df, index=['dat_pricedate'],
                              values=['num_price'], columns=['cod_product', "num_offset"])

    product_dict = dict()
    for product in "YQM":
        df_product = df_pivot.xs(product, level="cod_product", axis=1)
        df_product1_orig = df_product.xs(1, level="num_offset", axis=1)
        roll_expiration(df_product, product=product)
        df_product1_adj = df_product.xs(1, level="num_offset", axis=1)
        product_dict[f"{product}1"] = df_product1_adj.values.flatten()
        if show_plots:
            plt.figure()
            plt.plot(df_product.index, df_product1_orig.values, label="Original")
            plt.plot(df_product.index, df_product1_adj.values, label="Adjusted")
            plt.title(f"Product: {product}+1 (continuous)")
            plt.legend()
            mplcursors.cursor()
            plt.show(block=False)  # Does not stop the program but windows are closed when program ends

    df_all = pd.DataFrame(product_dict, index=df_product.index)
    return df_all


def update_all():
    for commodity_name, cdty_config in OmipConfig.commodity_config.items():
        logger.info(f"Updating downloaded data for {commodity_name}")
        df_downloaded = download_data(commodity_name)
        show_plots = True
        logger.info(f"Calculating continuous products for {commodity_name}")
        df_processed = get_continuous_products(df_downloaded, show_plots=show_plots)
        logger.info(f"Calculating optimal deals for {commodity_name}")
        # df_optimal = calculate_optimal_deals(df_processed, show_plots=show_plots)
        # df_optimal.to_pickle(get_filename(commodity_name, ext="pkl", processed=True))


if __name__ == '__main__':
    omip = OmipDownloader()
    print(omip.download(pd.Timestamp(2016, 1, 1)))
    #print(omip.download())
    omip.settlement_df.xs(["Power", "ES", "Y", 1], level=[])
#    update_all()
#    logger.info("Done")
