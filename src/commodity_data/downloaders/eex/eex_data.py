"""
File to read futures data from eex web site
"""
import json
import re
from pathlib import Path
from typing import List

import pandas
import pandas as pd
from bs4 import BeautifulSoup

from commodity_data import logger
from commodity_data.downloaders.base_downloader import HttpGet


def get_js_var(var_name: str, where: str) -> str:
    """Gets the string of the value of a javascript variable, like a = value"""
    found = re.findall(f"{var_name}.*\s?=\s?(.*);", where)
    if found:
        first_found = found[0]
        first_found = first_found.replace(",]", "]")
        return json.loads(first_found)
    return ""


class EEXData(HttpGet):
    """Class to get market data from eex"""

    format_year_month_day = "%Y/%m/%d"        # Date format of other date: year/month/day
    format_month_day_year = "%m/%d/%Y"        # Date format for global vision dates, moth/day/year
    commodities = "power", "natural-gas", "environmentals", "agriculturals", "freight"
    config_cache_file = Path(__file__).parent / "eex_market_config.csv"

    def __init__(self, force_download_config: bool=False):
        """
        Init the EEX Data class, reloading product configuration data from cache if possible
        :param force_download_config: ignore product configuration cache and force data reloading
        """
        super().__init__()
        self.logger = logger
        cache = self.load_check_cache() if not force_download_config else dict()
        self.cache_valid = cache.get('valid', False)
        self.cache_df = cache.get('df', None)
        self.market_config_df = self.get_market_futures_config_df()
        self.logger.debug(self.market_config_df.to_string())

    def js_request_results(self, url: str, params: dict, headers: dict = None) -> dict:
        self.headers = {
            # 'Accept': '*/*',
            # 'Accept-Language': 'es-ES,es;q=0.6',
            # 'Cache-Control': 'no-cache',
            # 'Connection': 'keep-alive',
            'Origin': 'https://www.eex.com',
            # 'Pragma': 'no-cache',
            'Referer': 'https://www.eex.com/',
        }
        if headers:
            self.headers.update(headers)
        response = self.http_get(url=url, params=params)
        return json.loads(response.data)['results']
        # response = requests.get(url, params, headers=default_headers)
        # response.raise_for_status()
        # return response.json()['results']

    def load_check_cache(self, max_old_days: int = 1) -> dict:
        """Checks if cache file exists and is less than 1 day old. Returns a dict with fields "valid", a boolean
         and "df", a dict that can be valid or not"""
        retval = dict(valid=False, df=None)
        if self.config_cache_file.exists():
            cache_df = pd.read_csv(self.config_cache_file, parse_dates=["min_date"])
            retval['df'] = cache_df
            min_date = pd.Timestamp.now().normalize() - pd.offsets.BDay(max_old_days)
            file_ts = self.config_cache_file.stat().st_mtime
            if file_ts >= min_date.timestamp():
                retval['valid'] = True
                # retval['valid'] = False   # For tests
        return retval

    def get_market_futures_config_df(self) -> pd.DataFrame:
        """
        Finds codes for the deliveries of all commodities defined in self.commodities,
        gets is initial date and the mapping of showing in the webpage
        If found in cache, reads from cache. Else downloads all info and writes it to the cache file
        :return: a pandas DataFrame
        """
        if self.cache_valid:
            self.logger.info("Reading EEX config market data from cache")
            return self.cache_df
        elif self.cache_df is None:
            self.logger.info("Reading EEX config market data from web site for the first time. This will take time")
        else:
            self.logger.info("Refreshing EEX config market data from web site")

        all_market_data = list()
        for commodity in self.commodities:
            # commodity: power, gas...
            self.logger.debug(f"Reading {commodity} futures data")
            res = self.http_get(f"https://www.eex.com/en/market-data/{commodity}/futures")
            soup = BeautifulSoup(res.data, features="lxml")
            picker = soup.find(id="snippetpicker")
            # find options among the options of the picker
            options = {op.text.strip(): op.attrs['value'] for op in picker.find_all("option")}
            for market, snippet_id in options.items():
                # Market: Spanish power futures, TTF Gas...
                self.logger.info(f"Reading config for '{commodity}': '{market}'")
                snippet = soup.find(id=f"snippet-{snippet_id}")
                fields = snippet.find_all("field")
                column_mapping = {field['name']: field['description'] for field in fields}
                script = snippet.find("script")
                all_symbols = set(re.findall("(\w*)Symbols_", script.text))
                deliveries = get_js_var("buttons", script.text)
                for symbol in all_symbols:
                    codes = get_js_var(f"{symbol}Symbols_", script.text)
                    for delivery, code in zip(deliveries, codes):
                        if not code:
                            continue
                        market_data = dict()
                        market_data['commodity'] = commodity
                        market_data['market'] = market
                        market_data['delivery'] = delivery
                        market_data['type'] = symbol
                        market_data['code'] = code
                        market_data['min_date'] = self.get_min_date(code)
                        market_data['column_mapping'] = json.dumps(column_mapping)
                        all_market_data.append(market_data)
        df = pd.DataFrame(all_market_data)
        df.to_csv(self.config_cache_file, index=False)
        self.cache_valid = True
        self.cache_df = df
        return df

    def get_min_date(self, eex_code: str) -> pd.Timestamp:
        """Gets info for a given eex_code, such as /E.FAPPJ24 or "/E.FAPP. Gets min date(when symbol was created)"""
        # Try to get from cache first. Min date should not change regularly and it is quite slow

        def get_date_from_cache(code: str):
            if code in self.cache_df['code'].values:
                return self.cache_df[self.cache_df['code'] == code]['min_date'].iat[0]
            else:
                return None

        if self.cache_df is not None:
            if retval := get_date_from_cache(eex_code):
                return retval
            if retval := get_date_from_cache(eex_code[:6]):        # If has product delivery spec, remote it
                return retval

        params = {
            'symbol': eex_code,
        }
        market_details = self.js_request_results('https://queryeex.gvsi.com/ExactSymbolSearch/json',
                                                 params=params)
        min_date = market_details[0]['result'][0]['dateCreated'][:10]
        return pd.Timestamp(min_date)

    def download_price_symbol_history(self, price_symbol: str, since: pd.Timestamp | str = None,
                                      to: pd.Timestamp = None) -> pd.DataFrame:
        """
        Downloads price_symbol history from the first date it was created till no
        :param price_symbol: a price symbol with date delivery specification such as ("/E.FALMK24")
        :param since: minium start date to download. Defaults to 45 business days prior to date. You can use
        a date or "all" to get data since product was created in EEX
        :param to: max date for the data, defaults to today
        :return:
        """

        to = to or pd.Timestamp.today()
        since = since or (pd.Timestamp.today() - pd.offsets.BDay(45))
        if isinstance(since, str):
            if since not in ("all", "max"):
                ValueError("Parameter since is not valid. Use 'all' or 'max' to get data since product was created")
            since = self.get_min_date(price_symbol)
        params = {
            'priceSymbol': f'"{price_symbol}"',
            'chartstartdate': since.strftime(self.format_year_month_day),
            'chartstopdate': to.strftime(self.format_year_month_day),
            'dailybarinterval': 'Days',
            'aggregatepriceselection': 'First',
        }

        history = self.js_request_results(
            'https://webservice-eex.gvsi.com/query/json/getDaily/close/offexchtradevolumeeex/'
            'onexchtradevolumeeex/tradedatetimegmt/openinterest/',
            params=params,
        )
        # print(history)
        df_history = pd.DataFrame.from_records(history['items'])
        return df_history

    def download_symbol_chain_table(self, symbol: str, date: pd.Timestamp, expiration_date: pd.Timestamp = None,
                                    use_mapping: bool = False) -> pd.DataFrame:
        """
        Downloads the symbol table (with the chain for all strips for a certain delivery defined by the symbol)
        :param symbol: eex simple code (e.g /E.EBEY)
        :param date: settlement date for the symbol
        :param expiration_date: optional, defaults to date. Probably it won't be never needed
        :param use_mapping: True to change original column names to eex site mappings. Defaults to False (keep original)
        :return: a pandas DataFrame with all the valid expirations for the given symbol
        """
        self.logger.info(f"Downloading data of {symbol} as_of {date}")
        expiration_date = expiration_date or (date - pd.offsets.Day(1))
        params = {
            'optionroot': f'"{symbol}"',
            'expirationdate': expiration_date.strftime(self.format_year_month_day),
            'ondate': date.strftime(self.format_year_month_day),
        }

        product_details = self.js_request_results(
            'https://webservice-eex.gvsi.com/query/json/getChain/gv.pricesymbol/gv.displaydate/gv.expirationdate/'
            'tradedatetimegmt/gv.eexdeliverystart/ontradeprice/close/onexchsingletradevolume/onexchtradevolumeeex/'
            'offexchtradevolumeeex/openinterest/',
            params=params
        )
        df = pd.DataFrame.from_records(product_details['items'])
        # No need to use column mapping
        if use_mapping:
            mapping = self.market_config_df[self.market_config_df['code'] == symbol]['column_mapping'].iat[0]
            mapping = json.loads(mapping)
            df.rename(columns=mapping, inplace=True)
        # Convert dates to pd.Timestamps
        for c in df.columns:
            if c.startswith("gv") and "date" in c:
                df[c] = pd.to_datetime(df[c], format=self.format_month_day_year)
            if c == 'gv.eexdeliverystart':
                df['maturity'] = pd.to_datetime(df[c].apply(lambda x: x.split(" ")[0]), format=self.format_month_day_year)
        return df

    def get_eex_config_df(self, market: str, delivery=None, type_=None) -> pandas.DataFrame:
        """Gets a filtered DataFrame with market, delivery, type and code columns
         of the EEX market symbol according to the given description (will return markets with that
         contain the given market, case-insensitive)
         regular expression) and optionally delivery and type (base/peak)"""
        def filter_df(df: pd.DataFrame, filter_column: str, filter_value: str) -> pd.DataFrame:
            if not filter_:
                return df
            df_filtered = df[df[filter_column].str.upper().str.contains(filter_value.upper())]
            return df_filtered

        df = self.market_config_df
        for column, filter_ in ("market", market), ("delivery", delivery), ("type", type_):
            df = filter_df(df, column, filter_)
        retval = df[['market', 'delivery', 'code']]
        return retval

    def get_eex_code(self, market: str, delivery=None, type_=None) -> List[str]:
        """Gets a list of the EEX market symbol according to the given description (will return markets with that
         contain the given market, case insensitive)
         regular expression) and optionally delivery and type (base/peak)"""
        df = self.get_eex_config_df(market, delivery, type_)
        return df['code'].to_list()


if __name__ == '__main__':
    eex = EEXData()

    symbol = eex.get_eex_config_df("Spanish", delivery="Day")
    daily_data = eex.download_symbol_chain_table(symbol=symbol['code'].values[0], date=pd.Timestamp(2024, 3, 18))

    config = eex.market_config_df
    milk = config[config['market'].str.contains("Liquid Milk")]
    milk = config[config['market'].str.contains("Spanish")]
    milk_code = milk['code'].iat[0]
    print(milk.to_string())
    print(milk_code)
    for as_of_date in pd.bdate_range("2024-03-01", "2024-03-20"):
        df_milk = eex.download_symbol_chain_table(symbol=milk_code, date=as_of_date)

        print(f"As_of: {as_of_date} has data: {not df_milk.empty}")

    import matplotlib.pyplot as plt

    # That's milk for may 24
    df_milk = eex.download_price_symbol_history("/E.FALMK24", since="all")
    print(df_milk)
    df_milk.set_index("tradedatetimegmt", inplace=True)
    df_milk.close.plot()
    plt.show()

    # Potato march 24
    df_milk = eex.download_price_symbol_history("/E.FAPPJ24", since="all")
    print(df_milk)
    df_milk.set_index("tradedatetimegmt", inplace=True)
    df_milk.close.plot()
    plt.show()

    # Whey powder march 24
    df_milk = eex.download_price_symbol_history("/E.FAWHH24", since="all")
    print(df_milk)
    df_milk.set_index("tradedatetimegmt", inplace=True)
    df_milk.close.plot()
    plt.show()
