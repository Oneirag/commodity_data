# Commodity_data
Downloads commodity data from the Futures markets operators. Currently, Omip (www.omip.pt), EEX (www.eex.com) and barchart (for ICE EUA, FX, Crypto and Stocks) are available
Data is stored in a ong_tsdb dabatase accessible through a configuration using ong_utils.
There are specific classes also to download data in pandas dataframe format from the same sources

## Detailed explanation of index columns:
                   
* **market**: Name of the market. Currently, Omip (ES, FR and DE baseload&peak, PVB), EEX (Spanish power, but others could be used) and Barchart (EUA, Stocks, Crypto, FX)
* **commodity**: Generic name of commodity (Power, Gas, CO2....)
* **instrument**: BL (baseload)/PK (peakload), EUA...
* **area**: Country (ES, FR, DE...)
* **product**: M, Q, Y, W, or D for month, quarter, year, week, day
* **offset**, number of relative products from each as_of to first delivery of the product (integer)
* **type**,  "close" for original prices, "continuous" for continuous prices adjusted rolling to next offset at
  expirations, "maturity" for the starting date of the delivery period as a timestamp

## Default downloaded data
See `commodity_data/downloaders/default_config.py` for the details of all default 
data that is downloaded.
Should you need additional data to be downloaded (with barchart), you'll need to modify the config file `commodity_data.yml`.

As a summary of default downloaded data:

### Omip
Downloads the following instruments for calendar Day, Month, Quarter and Year deliveries and baseload only: 
* Spanish BL Power Futures (market="Omip", commodity="Power", area="ES", instrument="BL")
* German BL Power Futures (market="Omip", commodity="Power", area="DE", instrument="BL")
* French BL Power Futures (market="Omip", commodity="Power", area="FR", instrument="BL")
* Spanish Gas Futures (market="Omip", commodity="Gas", area="ES", instrument="BL")
### Barchart
Downloads the following info:
* CO2 Emissions: with commodity="CO2", downloads EUA settle for december up to year + 4
* Cryptocurrencies: with commodity="Crypto", Ethereum and Bitcoin
* Fiat foreign exchange: with commodity="FX" Euro to Dolar and Euro to British Pound
* Stocks: with commodity="Stocks", Endesa, Ibex35 index and Apple
### EEX
* Spanish BL Power Futures (market="EEX", commodity="Power", area="ES", instrument="BL")


## Usage
### Standalone
You can download prices directly from the sources to pandas dataframes, see examples in the samples folder

### Prerequisites
#### Run underlying `ong_tsdb` database
This library relies on an [ong_tsdb](https://github.com/Oneirag/ong_tsdb.git) database that must be running and 
properly configured in the config files.

Visit github repo of [ong_tsdb](https://github.com/Oneirag/ong_tsdb.git) for instructions on how to run and configure a ong_tsdb database
(including how to run it in google colab)

#### Setup a `commodity_data.yml` config file
The program needs a config file, at least config connection to the required underlying 
[ong_tsdb](https://github.com/Oneirag/ong_tsdb.git) database.

The config file is located by default in `~/.config/ongpi`. This directory can be 
changed by setting the environment variable `ONG_CONFIG_PATH` to the required directory.

The minimal config file (`commodity_data.yml`) will be as follows
```yaml
commodity_data:
  url: http://localhost:5000      # edit to point to the ong_tsdb server address
  admin_token: whatever_is_in_ong_tsdb.yml
  write_token: whatever_is_in_ong_tsdb.yml
  read_token: whatever_is_in_ong_tsdb.yml
```
If you need to download additional data from barchart or your ong_tsdb database is behind an authentication proxy,
a more complex file will be needed. An example of file with all parameters is:
```yaml
commodity_data:
  url: http://localhost:5000
# Configuration of proxy authentication. Only for an ong_tsdb that runs under a server that needs additional authentication
# Passwords are stored in localhost keyring. See ong_utils in github for instructions on how to set up the keyring (if needed) 
  service_name_proxy : keyring_service_name_proxy
  service_name_google_auth: keyring_service_name_for_google_auth_secret
  proxy_username: keyring_username
# End of configuration fo proxy authentication
  admin_token: whatever_is_in_ong_tsdb.yml
  write_token: whatever_is_in_ong_tsdb.yml
  read_token: whatever_is_in_ong_tsdb.yml
  # Example of how to download additional stock data from barchart
  barchart_downloader:
    - commodity_cfg:    # Tags for ong_tsdb
        area: US
        commodity: Stock
        instrument: Apple
      download_cfg:
        symbol: AAPL      # Symbol name for barchart

  # Change barchart_downloader_use_default to false to ignore default values and use only the ones defined in this file
#  barchart_downloader_use_default: false

```

### Running `commodity_data`
#### Downloading/refreshing data
```python
from commodity_data.downloaders import OmipDownloader
omip = OmipDownloader()
omip.download()     # Downloads everything till today
omip.download("2022-01-01")     # Downloads everything from Jan 1st, 2022
```
#### Using already downloaded data
```python
from commodity_data.downloaders import OmipDownloader
omip = OmipDownloader()
omip_settle = omip.settlement_df
print(omip_settle) # Actual data. No need to invoke download()

# Plots evolution of settlement prices and adjusted settlement prices for cal ahead (offset=1) of Spanish power baseload
import matplotlib.pyplot as plt
plot_df = omip.settle_xs(market="Omip", commodity="Power", product="Y", offset=1)
plot_df.plot()
plt.show()
plot_df     # Show data
```
#### Downloading from barchart
To download EUA prices (commodity="CO2"), forex (commodity="FX"), cryptocurrencies (commodity="Crypto")
or stocks (commodity="Stock"):
```python
from commodity_data.downloaders import BarchartDownloader
import matplotlib.pyplot as plt 
barchart = BarchartDownloader()
barchart.download()
print(barchart.settlement_df)  # Actual data. No need to invoke download()
for commodity in ("CO2", "FX", "Crypto", "Stock"):
    # Plots evolution of settlement prices and adjusted settlement prices for selected market
    try:
        # For CO2 only dec deliveries are downloaded, so use offset=1 to get data for current dec
        barchart.settle_xs(commodity=commodity, offset=1).plot()
    except:
        # For the rest only spot data is downloaded, no future data at all
        barchart.settle_xs(commodity=commodity, offset=0).plot()
    plt.show()
    # mpld3.show(port=mpld3_port)

```
#### Downloading from EEX
```python
from commodity_data.downloaders import EEXDownloader
eex = EEXDownloader()


```

### Google Colab
First, mount google drive, install libraries and setup environment:
```python
# Mount drive
from google.colab import drive
drive.mount('/content/gdrive')

# Install commodity_data
!pip install git+https://github.com/Oneirag/commodity_data.git
# set environ variable to make config persistent
%env ONG_CONFIG_PATH=/content/gdrive/MyDrive/.config/ongpi
```
For the first execution, there is the need to create two configuration files, one for ong_tsdb (`ong_tsdb.yml`) and
other for commodity data (`commodity_data.yml`). 

See [ong_tsdb](https://github.com/Oneirag/ong_tsdb.git) repo for details on the file and above for sample file commodity_data.yml

In a separate cell start in the background the ong_tsdb server
```python
%%python3 --bg --out output

from ong_tsdb.server import main
main()
```

Then in a new cell you can start using the library as in the above examples

```python
from commodity_data.downloaders import OmipDownloader
omip = OmipDownloader()
omip.download("2022-01-01")     # omip.download() will download full history (very slow)
```