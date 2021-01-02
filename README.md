# Commodity_data
Downloads commodity data from the Futures markets operators. Currently only Omip (www.omip.pt)
Data is stored as a pandas Dataframe in a pickle per market, stored by default in `~/.ongpi/commodity_data` with columns stored in a MultiIndex
and indexed by as_of date

### Detailed explanation of index columns:
                   
* **market**: Name of the market (currently, only Omip)
* **commodity**: Generic name of commodity (Power, Gas, CO2....)
* **instrument**: BL (baseload)/PK (peakload), EUA...
* **area**: Country (ES, FR, DE...)
* **product**: MQYWD for month, quarter, year, week, day
* **offset**,  Distance in of products from each as_of to first delivery of the product 
* **type**,  "close" for original prices, adj_close for continuous prices adjusted rolling to next offset at expirations

## Usage
For downloading and refreshing data
```
from commodity_data import OmipDownloader
# or from commodity_data.omip import OmipDownloader
omip = OmipDownloader()
omip.download()     # Downloads everything till today
omip.download("2020-01-01")     # Downloads everything from Jan 1st, 2020
```
For using already downloaded data
```
from commodity_data import OmipDownloader
# or from commodity_data.omip import OmipDownloader
omip = OmipDownloader()
print(omip.settlement_df) # Actual data. No need to invoke download()
# Plots evolution of settlement prices and adjusted settlement prices for cal ahead of Spanish power baseload
import matplotlib.pyplot as plt
print(omip.settle_xs(market="Omip", commodity="Power", product="Y", offset=1).plot()
plt.show()
```

