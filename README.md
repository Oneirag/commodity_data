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
### Prerequisites
#### Run `ong_tsdb` database
This library relies on an [ong_tsdb](https://github.com/Oneirag/ong_tsdb.git) database that must be running and 
properly configured in the config files.

Visit github repo of [ong_tsdb](https://github.com/Oneirag/ong_tsdb.git) for instructions on how to run and configure a ong_tsdb database
(including how to run it in google colab)

#### Setup a `commodity_data.yml` config file
The program needs a config file to connect to the required underlying 
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
However, a more complex file can be created with additional parameters, if needed. An example of file with all parameters could be:
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
#### Downloading and refreshing data
```python
from commodity_data.downloaders import OmipDownloader
omip = OmipDownloader()
omip.download()     # Downloads everything till today
omip.download("2020-01-01")     # Downloads everything from Jan 1st, 2020
```
#### Using already downloaded data
```python
from commodity_data.downloaders import OmipDownloader
omip = OmipDownloader()
print(omip.settlement_df) # Actual data. No need to invoke download()
# Plots evolution of settlement prices and adjusted settlement prices for cal ahead of Spanish power baseload
import matplotlib.pyplot as plt
print(omip.settle_xs(market="Omip", commodity="Power", product="Y", offset=1))
plt.show()
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
```