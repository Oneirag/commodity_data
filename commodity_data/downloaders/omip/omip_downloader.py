from datetime import datetime
from typing import Union

import pandas as pd
from pandas import DataFrame, Series

from commodity_data.downloaders.base_downloader import BaseDownloader, TypeColumn, combine_config
from commodity_data.series_config import df_index_columns, OmipConfig
from commodity_data.downloaders.default_config import omip_cfg
from commodity_data.series_config import valid_product
from commodity_data import config
import marshmallow_dataclass


class OmipDownloader(BaseDownloader):

    def __init__(self, name="Omip"):
        """
        Creates a barchart downloader.
        For the list of symbols to read, uses a default configuration defined in
        commodity_data.downloaders.default_config.py, that can be extended using configuration file with two keys:
        - omip_downloader: where a json/yaml configuration following OmipConfig marshmallow spec defined in
        series_config can be used
        - omip_downloader_replace: True/False value to define whether the configuration must extend the default
        (False, default value) or replace (if True) the available configuration
        :param name: The name of the sensor where data will be stored/read from (Omip as a default)
        """
        cfg = config("omip_downloader", dict())
        omip_parser = marshmallow_dataclass.class_schema(OmipConfig)()
        self.config = combine_config(omip_cfg, cfg, omip_parser,
                                     use_default=config("omip_downloader_use_default", True))
        super().__init__(name=name)
        # Calculate the absolute minimum date for download
        self.__min_date = min(pd.Timestamp(cfg.download_cfg.start_t) for cfg in self.config)

    def min_date(self):
        return self.__min_date

    def _download_date(self, as_of: pd.Timestamp) -> pd.DataFrame:
        dfs = list()
        # for cdty, cdty_config in OmipConfig.commodity_config.items():
        for cfg in self.config:
            cdty = cfg.commodity_cfg.commodity
            self.logger.info(f"Downloading {cdty} for date {self.as_of_str(as_of)}")
            df = self.__download_omip_data(self.as_of_str(as_of), **cfg.download_cfg.__dict__)
            if df is None or df.empty:
                continue        # Skip if empty or None
            for c in df_index_columns:
                if c in cfg.commodity_cfg.__dict__:
                    df[c] = getattr(cfg.commodity_cfg, c)
            df['market'] = self.name()
            df['type'] = TypeColumn.close.value

            df = df.drop(columns=['maturity'])
            df = pd.pivot_table(df, values="close", index="as_of", columns=df_index_columns)
            dfs.append(df)
        if dfs:
            return pd.concat(dfs, axis=1)
        else:
            return None

    def __download_omip_data(self, as_of: str, instrument="FTB", product="EL", zone="ES", **kwargs) -> Union[
        None, DataFrame, Series]:
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
            tables = (t.dropna(axis=0, how="all") for t in pd.read_html(req.data, decimal="."))
        except ValueError:
            # No tables found in website
            return None
        valid_tables = list()
        for table in tables:
            table = table.drop(index=0)
            table.set_index(table.columns[0], inplace=True)
            product = table.index[0].split(instrument)[1].strip()
            if product[0] in valid_product:
                table.index = pd.MultiIndex.from_tuples(
                    list(parse_omip_product_maturity_offset(idx.split(instrument)[-1].strip(), as_of_ts)
                         for idx in table.index),
                    names=["product", "maturity", "offset"])
                table = table.drop(columns=(n for n in table.columns if n != "Reference prices"))
                table["close"] = pd.to_numeric(table["Reference prices"], errors='coerce')
                table = table.dropna(axis=0, how="any")
                if table.empty:
                    self.logger.debug(f"No valid data for {as_of}, returning None")
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


if __name__ == '__main__':
    import matplotlib.pyplot as plt
    omip = OmipDownloader()
    omip.load()
    omip.settle_xs(commodity="Power", area="ES", product="Y", offset=1).plot()
    plt.show()
    # omip.load()
    print(omip.download(pd.Timestamp(2019, 7, 1)))
    omip.roll_expiration()
    omip.load()
    # print(omip.download(pd.Timestamp(2016, 1, 1)))
    #print(omip.download())
    omip.settle_xs(commodity="Power", area="ES", product="Y", offset=1).plot()
    plt.show()

#    update_all()
#    logger.info("Done")
