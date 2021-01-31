from commodity_data.downloaders.base_downloader import BaseDownloader, df_index_columns
import urllib.parse
import io
import pandas as pd
import numpy as np
from commodity_data.downloaders.barchart import barchart_config



class BarchartDownloader(BaseDownloader):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        """
        Host: www.barchart.com
        User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0
        Accept: application/json
        Accept-Language: en-US,en;q=0.5
        Accept-Encoding: gzip, deflate, br
        X-XSRF-TOKEN: eyJpdiI6Ikt1ZVhLR1k2TVV3K0V1azRMQWptRnc9PSIsInZhbHVlIjoib2V3aDhkK3NUbkFpYnN2V3RHR3g1ZWtSRGh1ejMxaDhWcEdWZkY0TWRvbC8xenVPU0JCN25zQ1Z6RHdFOEYvTSIsIm1hYyI6IjQ2MWM0ZmNmOTk3YjViY2JmYjY4YjE4ZjQyOTVhN2UwYzRkYTI0YWNiNTgwNWE3MDZkMWQ2MDk4MDg0YzVhZmIifQ==
        Connection: keep-alive
        Referer: https://www.barchart.com/futures/quotes/CKZ21/overview
        Cookie: _gcl_au=1.1.804883759.1609659799; _ga=GA1.2.612794240.1609659801; usprivacy=1---; _awl=2.1611723624.0.4-380f457e-4464bbc6c40fb06cdc140e229a16c060-6763652d6575726f70652d7765737431-6010f368-0; _admrla=2.; tcf2cookie=CO_bpgPO_bpgPAJAEBENBHCsAP_AAEPAACiQHONd_X_fb39j-_59_9t0eY1f9_7_v-0zjgeds-8Nyd_X_L8X42M7vF36pq4KuR4Eu3LBIQFlHOHUTUmw6IkVrTPsak2Mr7NKJ7PEinMbe2dYGHtfn9VTuZKYr97s___z__-__v__75f_r-3_3_vp9X---_e_QOXAJMNS-AizEscCSaNKoUQIQriQ6AUAFFCMLRNYQErgp2VwEfoIGACA1ARgRAgxBRiwCAAAAAJKIgJADwQCIAiAQAAgBUgIQAEaAILACQMAgAFANCwAigCECQgyOCo5TAgIkWignkjAEou9jDCEMosAKBR_QAAA.f_gACHgAAAAA; pubcv=%7B%7D; kppid_managed=kppidff_N3K_soyV; __gads=ID=a732fa86c5618e91-229a467397a6003f:T=1609659820:S=ALNI_MaCM75bjswmv5a7rx_81KGBd6C7MQ; cto_bundle=pfTL9V9pV3hYa2xkdlozMW1MSXB2WG5RNTdVRXlJUG1UeE13ejlZeDgxaDZZRzdVWDlsRmxvSEF1ZVRmTXhLdDZWVllhJTJGazEzdURuYWtYbnRPSjhBd3l4STR5cTBnblh1RGduV0tJZlBrTEVRRTF0SlJEeEMlMkZmY25PMXc1UzRXZlpYemlOOWZxY1hyVEZsQjAyNWtvSUhFMmJ3JTNEJTNE; _pbjs_userid_consent_data=536950611269135; id5id.1st_212_nb=0; SKpbjs-id5id=%7B%22created_at%22%3A%222021-01-03T07%3A45%3A50.321Z%22%2C%22id5_consent%22%3Atrue%2C%22original_uid%22%3A%22ID5-ZHMOuK7Iyh0BhGFsmb6UGzcJQMHV3NeNMyU9KtUWRw%22%2C%22universal_uid%22%3A%22ID5-ZHMOuK7Iyh0BhGFsmb6UGzcJQMHV3NeNMyU9KtUWRw%22%2C%22signature%22%3A%22ID5_ATo2FMbg2V7lBqyyZd9yQJfdCu4_ZdcMRuIEy-_OvtK5UKbqlgmXoXerMkVUisvUwYOX_9AOL0VneRtO-rOWrNE%22%2C%22link_type%22%3A0%2C%22cascade_needed%22%3Atrue%7D; SKpbjs-id5id_last=Sun%2C%2003%20Jan%202021%2007%3A45%3A50%20GMT; SKpbjs-unifiedid=%7B%22TDID%22%3A%223c110f2f-4dfa-4d70-bd7c-3205acabd900%22%2C%22TDID_LOOKUP%22%3A%22FALSE%22%2C%22TDID_CREATED_AT%22%3A%222021-01-03T07%3A45%3A50%22%7D; SKpbjs-unifiedid_last=Sun%2C%2003%20Jan%202021%2007%3A45%3A50%20GMT; laravel_token=eyJpdiI6Ik5GV25GenN0U2RnbnZkRnQxTTF6VEE9PSIsInZhbHVlIjoiWFJaNlR3SzVBdDJUZFN0bzRjU1hwNEREbTlpVXV3S1d4cWp2MXFKNENhaTJqdW13OFNhYWdNYkt0NVJNblZPa2FaK1VBREt5N0ZlOGVZRC9hcjlONVZvalI5b0dNZzhmR2Jkb0N6ZHYwVzJLZHhwRjV4NVJ4eU0yMkxtQkpNZVhxRllDNEtkNVpBemNubXpSaEljMTJ3MWx4ZWJRK2FGY01QV2hKdU1SdklCL3o4Z3Z6VDFHbzRZM2ZIVmFXRlI0NjNkbFpWakk3Q1ljeW9IamczN2R3d0s2dzJkUXdyMElzRTVNdDZJOVJmQkpHMjZHaXh0SjZuRDR1UzhRVTJyTCIsIm1hYyI6ImQzM2E1MjBjMDU5ZGIwYjJiMGVmYTI3YWE4MGJjYmUzNTAwNTZjNzczMjM3ZGQ4YzI1OWM0ODg4OWVjMGZhMjUifQ%3D%3D; XSRF-TOKEN=eyJpdiI6Ikt1ZVhLR1k2TVV3K0V1azRMQWptRnc9PSIsInZhbHVlIjoib2V3aDhkK3NUbkFpYnN2V3RHR3g1ZWtSRGh1ejMxaDhWcEdWZkY0TWRvbC8xenVPU0JCN25zQ1Z6RHdFOEYvTSIsIm1hYyI6IjQ2MWM0ZmNmOTk3YjViY2JmYjY4YjE4ZjQyOTVhN2UwYzRkYTI0YWNiNTgwNWE3MDZkMWQ2MDk4MDg0YzVhZmIifQ%3D%3D; laravel_session=eyJpdiI6Ikg3Y1VURjh0MDkxZ0dFeFlmTzE5REE9PSIsInZhbHVlIjoiU01zNkQzZ0hTRW9hMUxhQkJ2Q1V2Q2pDMUovTUZoZHQwMGZxU1dZT3RYeXdPdTJSNmc0L1h2SzV1dDRwZm5RSyIsIm1hYyI6Ijg1MzlhY2ZiOGNhNGFiMWJiZTdjZmZiNWRiYjA3MmQzNzg3ZTgxYWRjYTM4ZTdhZDcxMjAwOWZjZTIyMWMzOTgifQ%3D%3D; market=eyJpdiI6Ikg0cWhOVDQ5ZGFRY3ZLbnYxekU2cmc9PSIsInZhbHVlIjoiUCsxSHVhSlpIOURaQlNxMG5VYWRTdz09IiwibWFjIjoiNDk4ODJlZDM5NzEwMDU0OTZiNWMxNGZiMzQyZWJiNWRlNDMyYWVkMGVkYWVlOWI3NWU1MjNlMTA1Mjk2YjMwNiJ9; bull-puts-01272021PageView=1; bull-puts-01272021WebinarClosed=true; _gid=GA1.2.1537025805.1611723139; IC_ViewCounter_www.barchart.com=3; _gat_UA-2009749-51=1
        TE: Trailers"""
        self.headers = {"Host": "www.barchart.com",
                        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0"}
        self.cookies = {}
        resp = self.http_get("https://www.barchart.com/futures/quotes/CKZ21/overview")
        # Update cookies
        self.cookies = resp.cookies
        self.headers.update({"x-XSRF-TOKEN".lower(): urllib.parse.unquote(resp.cookies["XSRF-TOKEN"])})
        pass

    def file_name(self):
        return "ICE"

    def min_date(self):
        return pd.Timestamp(2013, 1, 1)

    def download(self, start_date: pd.Timestamp = None, end_date: pd.Timestamp = None) -> int:
        # refesh cache
        start_date = start_date or self.min_date()
        for symbol, config in barchart_config.items():
            params = dict(symbol=symbol,
                          data="daily",
                          maxrecords=np.busday_count(start_date.date(), pd.Timestamp.today().date()) + 1,
                          volume="contract",
                          order="asc",
                          dividends="false",
                          backadjust="false",
                          daystoexpiration=1,
                          contractroll="expiration"
                          )
            resp = self.http_get("https://www.barchart.com/proxies/timeseries/queryeod.ashx", params=params)
            df = pd.read_csv(io.StringIO(resp.content.decode('utf-8')))
            df.columns = ["symbol", "as_of", "open", "high", "low", "close", "volume", "oi"]
            df.as_of = pd.to_datetime(df.as_of)
            df = df.loc[:, ('close', "as_of")]
            df['market'] = self.file_name()
            for column_name, column_value in config['config'].to_dict().items():
                df[column_name] = column_value
            df['product'] = "Y"
            df['type'] = "close"
            df['maturity'] = pd.to_datetime(config['expiry'])
            # df = df.set_index("as_of")
            self.cache[symbol] = df

        return super().download(start_date, end_date)

    def _download_date(self, as_of: pd.DataFrame) -> pd.DataFrame:
        dfs = list()
        for symbol, df in self.cache.items():
            df = df[df['as_of'] == as_of]
            if not df.empty:
                # These two approaches causes a Settinng vs Copy warning
                # df.loc[df.index, 'offset'] = df['maturity'].dt.year - as_of.year
                # df['offset'] = df['maturity'].dt.year - as_of.year
                df = df.assign(offset=(df['maturity'].dt.year - as_of.year).values)  # assign values to column 'c'
                df = df.drop(columns=['maturity'])
                df = pd.pivot_table(df, values="close", index="as_of", columns=df_index_columns)
                dfs.append(df)
        if dfs:
            return pd.concat(dfs, axis=1)
        else:
            return None


if __name__ == '__main__':
    barchart = BarchartDownloader()
    barchart.download()
    barchart.settle_xs(offset=1).plot()
    import matplotlib.pyplot as plt
    plt.show()
    pass
