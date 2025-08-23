"""
Downloads all EEX & Omip data plus CO2, Brent, Henry Hub and FX, and generates an "algo_trading.csv" file
"""
import pandas as pd

from commodity_data import CommodityData
from commodity_data.utils.continous_price import roll_dfmi
from commodity_data.utils.contract_size import Duration, size_dfmi

cdty = CommodityData()

column_types = ["close", 'maturity']

# 1 - CO2
co2_df_1_2 = cdty.settle_xs(markets="Barchart", commodity="CO2", area=['EU'], offset=(1,2), product=["Y"], type=column_types)
size_dfmi(co2_df_1_2, 1000)

# 2 - Brent: offset must be 2 or 3, offset 1 does not work properly
brent_df_2_3 = cdty.settle_xs(markets="Barchart", commodity="Oil", offset=(2,3), product=["M"], type=column_types)
size_dfmi(brent_df_2_3, 1000)
# 3 - Henry hub: offset must be 2 or 3, offset 1 does not work properly
hh_df_2_3 = cdty.settle_xs(markets="Barchart", commodity="Gas", offset=(2,3), product=["M"], type=column_types)
size_dfmi(hh_df_2_3, 10000)
# 4 - Forex (USD/EUR)
fx_df = cdty.settle_xs(markets="Barchart", commodity="FX", instrument="EURUSD", offset=0, type=column_types)
size_dfmi(fx_df, 1000)

# 5 - Power and Gas, in Spain, France and Germany, just for offsets 1 and 2
power_df_1_2 = cdty.settle_xs(markets=["EEX", "Omip"], commodity=["Power", "Gas"], area=['ES', 'FR', 'DE'], offset=(1,2), 
                              product=["Y", "Q", "M"], type=column_types)
# Set size according to actual size and maturity
size_dfmi(power_df_1_2, Duration.hours_year, dict(product="Y"))
size_dfmi(power_df_1_2, Duration.hours_quarter, dict(product="Q"))
size_dfmi(power_df_1_2, Duration.hours_month, dict(product="M"))

df = pd.concat([co2_df_1_2, power_df_1_2, fx_df, brent_df_2_3, hh_df_2_3], axis=1).loc["2018-01-01":]
# Perform actual rolling
roll_dfmi(df)

df2 = pd.DataFrame(df.values, columns=["_".join(str(level) for level in c) for c in df.columns], index=df.index)
# Rename type="close" to type="price"
df2.columns = (c.replace("_close", "_price") for c in df2.columns)

df2.reset_index(inplace=True)
df2.sort_index(axis=1, inplace=True)

df2.to_csv("algo_trading.csv", date_format="%Y-%m-%d", index=False)

pass