import pandas as pd
from commodity_data.series_config import CommodityCfg

barchart_config = {
    "CKZ{}".format(str(year)[-2:]): {
        "expiry": pd.Timestamp(year + 1, 1, 1),
        "config": CommodityCfg("CO2", "EUA", "EU")
    }
    for year in range(2013, pd.Timestamp.today().year + 3)
}
