""""
Examples on how to download data
"""
from commodity_data import CommodityData

######################
# Creating downloader (Creates also the underlying database if not existing)
######################
# Option 2: Create the downloader, but skipping rolling expirations (a bit faster, but option 1 is preferred)
cdty = CommodityData(roll_expirations=False)

# Option1: create the global downloader
cdty = CommodityData()

#######################
# Downloading data
#######################
# updating all data until yesterday (best choice if you just want to update data)
cdty.download_all_yesterday()

# Downloading for a market in a certain date. It won't download again if data existed
cdty.download("2024-3-1", "2024-03-15", markets="EEX")

# Downloading for a market in a certain date. Force download all again even if data existed
cdty.download("2024-3-1", "2024-03-07", markets="EEX", force_download=True)

# Force download all history of a certain product
if "yes" == input("Type 'yes' to download again weeks from EEX: "):
    cdty.download(markets="EEX", force_download=dict(product="W"))  # Download all weeks from EEX
if "yes" == input("Type 'yes' to download again all power data from Omip: "):
    # Force download all power from OMIP
    cdty.download(markets="Omip", start_date="2015-01-01", force_download=dict(product="EL"))

