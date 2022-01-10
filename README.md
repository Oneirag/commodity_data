# Commodity_data
Downloads commodity data from the Futures markets operators. Currently, Omip (www.omip.pt) and barchart (for ICE EUA, FX, Crypto and Stocks) are available
Data is stored in a ong_tsdb dabatase accessible through a configuration using ong_utils.

### Detailed explanation of index columns:
                   
* **market**: Name of the market. Currently, Omip (ES, FR and DE baseload&peak, PVB) and Barchasrt (EUA, Stocks, Crypto, FX)
* **commodity**: Generic name of commodity (Power, Gas, CO2....)
* **instrument**: BL (baseload)/PK (peakload), EUA...
* **area**: Country (ES, FR, DE...)
* **product**: M, Q, Y, W, or D for month, quarter, year, week, day
* **offset**,  number of relative products from each as_of to first delivery of the product 
* **type**,  "close" for original prices, "adj_close" for continuous prices adjusted rolling to next offset at expirations

## Usage
####Downloading and refreshing data
```python
from commodity_data.downloaders import OmipDownloader
omip = OmipDownloader()
omip.download()     # Downloads everything till today
omip.download("2020-01-01")     # Downloads everything from Jan 1st, 2020
```
####Using already downloaded data
```python
from commodity_data.downloaders import OmipDownloader
omip = OmipDownloader()
print(omip.settlement_df) # Actual data. No need to invoke download()
# Plots evolution of settlement prices and adjusted settlement prices for cal ahead of Spanish power baseload
import matplotlib.pyplot as plt
print(omip.settle_xs(market="Omip", commodity="Power", product="Y", offset=1))
plt.show()
```