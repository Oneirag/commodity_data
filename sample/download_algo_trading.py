"""
Downloads all EEX & Omip data plus CO2, Brent, Henry Hub and FX, and generates an "algo_trading.csv" file
"""
import pandas as pd
from commodity_data import CommodityData

cdty = CommodityData()

column_types = ["close", 'maturity']

# CO2
co2_df_1_2 = cdty.settle_xs(markets="Barchart", commodity="CO2", area=['EU'], offset=(1,2), product=["Y"], type=column_types)
# Brent: offset must be 2 or 3, offset 1 does not work properly
brent_df_2_3 = cdty.settle_xs(markets="Barchart", commodity="Oil", offset=(2,3), product=["M"], type=column_types)
# Henry hub: offset must be 2 or 3, offset 1 does not work properly
hh_df_2_3 = cdty.settle_xs(markets="Barchart", commodity="Gas", offset=(2,3), product=["M"], type=column_types)

fx_df = cdty.settle_xs(markets="Barchart", commodity="FX", instrument="EURUSD", offset=0, type=column_types)

# Electricidad y gas de españa, francia y alemania offset 1 y 2
power_df_1_2 = cdty.settle_xs(markets=["EEX", "Omip"], commodity=["Power", "Gas"], area=['ES', 'FR', 'DE'], offset=(1,2), 
                              product=["Y", "Q", "M"], type=column_types)

df = pd.concat([co2_df_1_2, power_df_1_2, fx_df, brent_df_2_3, hh_df_2_3], axis=1).loc["2018-01-01":]

df2 = pd.DataFrame(df.values, columns=["_".join(str(level) for level in c) for c in df.columns], index=df.index)

df2.to_csv("algo_trading.csv", date_format="%Y-%m-%d")

pass