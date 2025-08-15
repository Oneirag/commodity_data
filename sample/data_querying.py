"""
Examples on how to query already downloaded data
"""
import pandas as pd

from commodity_data import CommodityData


def plot(df: pd.DataFrame, use_pdld3: bool = False):
    """Shows plot, using mpld3 if installed, otherwise using matplotlib"""
    ax = df.plot()
    if use_pdld3:
        try:
            fig = ax.get_figure()
            import mpld3
            from mpld3 import plugins
            # add tooltips
            plugins = [mpld3.plugins.LineLabelTooltip(line, label=name) for line, name in zip(ax.get_lines(),
                                                                                              df.columns)
                       ]
            # plugins = [mpld3.plugins.InteractiveLegendPlugin(line) for line, name in zip(ax.get_lines(),
            #            df.columns)
            #            ]
            mpld3.plugins.connect(fig, *plugins)
            mpld3.show()
            return
        except:
            pass
    import matplotlib.pyplot as plt
    plt.show()


cdty = CommodityData()

###
# Gets full data as a pandas dataframe, with as_of dates as indexes
###
date_from = pd.Timestamp.today().normalize() - pd.offsets.YearBegin(10)  # Since start of year-5
full_data = cdty.data(date_from)
print(full_data.head(5))
# Save it to a CSV file, in tabular format (for a standard database table)
tabular_data = cdty.data_stack(date_from, market=["EEX", "Omip"])
tabular_data.to_csv("all_data.csv")

###
# Filtering data. Use settle_xs to filter columns that will be returned. It returns all not None rows
###
# This will return Omip and EEX data for the prompt year and Quarter
df = cdty.settle_xs(commodity="Power", area="ES", product=["Y", "Q"], offset=1, type="close")
plot(df)
# This returns closes for both Omip and EEX data, just for the prompt year
df = cdty.settle_xs(commodity="Power", area="ES", product="Y", offset=1, type="close")
plot(df)
# This returns adj_closes for both Omip and EEX data for the prompt year
# adj_closes adjusts closes of products so there are no steps on product expirations, as it
# assumes that the product is rolled on expiration
df = cdty.settle_xs(commodity="Power", area="ES", product="Y", offset=1, type="adj_close")
plot(df)
