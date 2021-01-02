"""
Configuration for a simple commodity series and instruments
Examples:
    market commodity   instrument  Area            Product     Maturity
    Omip    Power       Base/Peak   Es/Pt/De/Fr     M/W/Y/Q     ---
    Omip    Emissions   EUA         Eu              Dec         ---
    Omip    Gas         Base        Es              MQY         ---
"""
import pandas as pd

df_index_columns = ["market",  # market from which data is downloaded (Omip, ICE...)
                    "commodity",  # Generic name of commodity (Power, Gas, CO2....)
                    "instrument",  # BL (baseload)/PK (peak load), EUA...
                    "area",  # Country
                    "product",  # D/W/M/Q/Y for calendar day/week/month/quarter/year
#                    "maturity",  # Maturity of the product (start date of delivery)
                    "offset",  # Number of calendar products of interval from as_of date till maturity
                    "type",     # close, adj_close...
                    ]
df_data_columns = ["close",  # Original settlement price
                   "adj_close"  # Adjusted settlement price (taking into account product rolling)
                   ]


class CommodityCfg:
    def __init__(self, commodity: str, instrument: str, area: str):
        self.commodity = commodity
        self.instrument = instrument
        self.area = area

    def __str__(self):
        return ".".join([self.commodity, self.instrument, self.area])


class InstrumentCfg:
    def __init__(self, product: str, maturity: pd.Timestamp):
        self.product = product
        if product not in "YMQD":
            raise ValueError(f"Product {product} not understood")
        self.maturity = maturity

    def __str__(self):
        return ".".join([self.product, self.date.strftime("%Y-%M-%d")])
