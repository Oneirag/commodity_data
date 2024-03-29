""""
Examples on how to download data
"""
from commodity_data.downloaders.downloader import CommodityDownloader

######################
# Creating downloader (Creates also the underlying database if not existing)
######################
# Option 2: Create the downloader, but skipping rolling expirations (a bit faster, but option 1 is preferred)
df = CommodityDownloader(roll_expirations=False)

# Option1: create the global downloader
dl = CommodityDownloader()

#######################
# Downloading data
#######################
# updating all data until yesterday (best choice if you just want to update data)
dl.download_all_yesterday()

# Downloading for a market in a certain date. It won't download again if data existed
dl.download("2024-3-1", "2024-03-15", markets="EEX")

# Downloading for a market in a certain date. Force download all again even if data existed
dl.download("2024-3-1", "2024-03-15", markets="EEX", force_download=True)

# Force download all history of a certain product
df.download(markets="EEX", force_download=dict(product="W"))
