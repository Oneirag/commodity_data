import pandas as pd

default_config = {
    "Omip": [
        # Spanish BL Power Futures
        {
            "download_cfg": {
                "instrument": "FTB", "product": "EL", "zone": "ES",
                # "start_t": "2006-01-01",
                "start_t": "2006-07-03",  # There is no data available before that date
            },
            "commodity_cfg": {
                "commodity": "Power", "instrument": "BL", "area": "ES",
            },
        },
        # German BL Power Futures
        {
            "download_cfg": {
                "instrument": "FDB", "product": "EL", "zone": "DE",
                "start_t": "2016-05-13",
            },
            "commodity_cfg": {
                "commodity": "Power", "instrument": "BL", "area": "DE",
            },
        },
        # French BL Power Futures
        {
            "download_cfg": {
                "instrument": "FFB", "product": "EL", "zone": "FR",
                "start_t": "2016-05-13",
            },
            "commodity_cfg": {
                "commodity": "Power", "instrument": "BL", "area": "FR",
            },
        },
        # Spanish Gas Futures
        {
            "download_cfg": {
                "instrument": "FGE", "product": "NG", "zone": "ES",
                "start_t": "2017-11-24",
            },
            "commodity_cfg": {
                "commodity": "Gas", "instrument": "BL", "area": "ES",
            },
        },

    ],
    "Barchart": [
        ###################
        # CO2 Emissions
        ###################
        *[
            {
                "download_cfg": {
                    "symbol": "CKZ{}".format(str(year)[-2:]),
                    "expiry": pd.Timestamp(year, 12, 20).strftime("%Y-%m-%d"),
                    "product": "Y",
                },
                "commodity_cfg": {
                    "commodity": "CO2",
                    "instrument": "EUA",
                    "area": "EU",
                },
            }
            for year in range(2013, pd.Timestamp.today().year + 4)
        ],
        ######################
        # Cryptocurrencies
        ######################
        {
            "download_cfg": {
                "symbol": '^ETHUSD',  # Ethereum
            },
            "commodity_cfg": {
                "commodity": "Crypto",
                "instrument": "ETH",
            },
        },
        {
            "download_cfg": {
                "symbol": '^BTCUSD',  # Bitcoin
            },
            "commodity_cfg": {
                "commodity": "Crypto",
                "instrument": "BTC",
            },
        },
        ####################
        # Fiat FX
        ####################
        {
            "download_cfg": {
                "symbol": '^EURUSD',  # Euro Dollar
            },
            "commodity_cfg": {
                "commodity": "FX",
                "instrument": "EURUSD",
            },
        },
        {
            "download_cfg": {
                "symbol": '^EURGBP',  # Euro British Pound
            },
            "commodity_cfg": {
                "commodity": "FX",
                "instrument": "EURGBP",
            },
        },
        #########################
        # Stocks, indexes, ETF...
        #########################
        {
            "download_cfg": {
                "symbol": 'ELE.E.DX',  # Endesa S.A.
            },
            "commodity_cfg": {
                "commodity": "Stock",
                "instrument": "Endesa",
                "area": "ES",
            },
        },
        {
            "download_cfg": {
                "symbol": '$IBEX',  # Ibex 35
            },
            "commodity_cfg": {
                "commodity": "Stock",
                "instrument": "Ibex35",
                "area": "ES",
            },
        },
        {
            "download_cfg": {
                "symbol": 'AAPL',  # Apple Inc
            },
            "commodity_cfg": {
                "commodity": "Stock",
                "instrument": "Apple",
                "area": "US",
            },
        },
    ],
    "EEX": [
        *[
            {
                "download_cfg": {
                    "instrument": instrument, "product": product,
                },
                "commodity_cfg": {
                    "commodity": "Power", "instrument": "BL", "area": "ES",
                },
            }
            for instrument, product in [
                ("/E.FEBY", "Y"),  # Spanish Baseload year
                ("/E.FEBM", "M"),  # Spanish Baseload month
                ("/E.FEBQ", "Q"),  # Spanish Baseload Quarter
                ("/E.FE_DAILY", "D"),  # Spanish Baseload Day
                ("/E.FEB_WEEK", "W"),  # Spanish Baseload Week
            ]
        ]
    ],
    "Esios": [
        *[{
            "commodity_cfg": {
                "commodity": "Power", "instrument": "BL", "area": area,
            },
            "download_cfg": {
                "indicator": 600, "column": column,
            }
        }
            for area, column in [
                ("ES", "España"),
                ("FR", "Francia"),
                ("DE", "Alemania"),
                ("PT", "Portugal"),
            ]
        ]
    ]
}
