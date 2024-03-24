import pandas as pd

from commodity_data.downloaders.base_downloader import HttpGet
from commodity_data.downloaders.offsets import date_offset
from commodity_data import logger
from commodity_data.series_config import valid_product
from datetime import datetime


def parse_omip_product_maturity_offset(omip_product: str, as_of: pd.Timestamp) -> tuple:
    """Gets a 3 element tuple of :
        Item 0: product name ("Y" for year, "Q" for quarter, "M" for month and "D" for year)
        Item 1: its maturity (start date of delivery) from an Omip product description
        Item 2: its offset from the start date
    If product can not be parsed returns None, None, None"""
    if omip_product.startswith("M"):
        date_str = omip_product[2:]
        maturity = pd.Timestamp(datetime.strptime(date_str, "%b-%y"))
        offset = date_offset(as_of, maturity, "M")
    elif omip_product.startswith("D"):
        date_str = omip_product[2:]
        maturity = pd.Timestamp(datetime.strptime(date_str[2:], "%d%b-%y"))
        offset = date_offset(as_of, maturity, "D")
    elif omip_product.startswith("Y"):
        year = int(omip_product[-2:])
        maturity = pd.Timestamp(year=(2000 + year), month=1, day=1)
        offset = date_offset(as_of, maturity, "Y")
    elif omip_product.startswith("Q"):
        date_str = omip_product
        quarter = int(date_str[1])
        year = int(date_str[3:])
        maturity = pd.Timestamp(year=(2000 + year), month=quarter * 3 - 2, day=1)
        offset = date_offset(as_of, maturity, "Q")
    else:
        return None, None, None

    return omip_product[0], maturity, offset


class Omip_Data(HttpGet):
    logger = logger
    """Class to download data directly from omip website"""

    def download_omip_data(self, as_of: str, instrument="FTB", product="EL", zone="ES", **kwargs) -> (
            None | pd.DataFrame | pd.Series):
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
                    return None  # No valid tables found
                table = table.reset_index()
                table = table.drop(columns=["Reference prices"])
                valid_tables.append(table)

        df = pd.concat(valid_tables)
        df['as_of'] = pd.Timestamp(as_of)
        return df


if __name__ == '__main__':
    omip = Omip_Data()
    print(df := omip.download_omip_data(as_of=pd.Timestamp.today().normalize() - pd.offsets.BDay(1)))
