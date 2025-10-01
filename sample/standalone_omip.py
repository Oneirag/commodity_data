"""
Example to download Omip data standalone, without writing to any database
In the case of Omip, data has to be downloaded per day and product
"""
import pandas as pd

from commodity_data.downloaders.omip import OmipData

if __name__ == '__main__':

    as_of = pd.Timestamp("2024-03-18")  # Sample day for downloads
    as_of = pd.Timestamp("2024-03-27")  # Sample day for downloads

    # Initialize object.
    omip = OmipData()

    """
    Downloading daily symbols from Omip
    """
    df_spanish_power_day = omip.download_omip_data(as_of=as_of, instrument="FTB", product="EL", zone="ES")
    print(df_spanish_power_day)

    # Download data between two dates. It has to be downloaded day by day
    downloaded_data = list()
    for as_of_date in pd.bdate_range(as_of - pd.offsets.BDay(5), as_of):
        df_ftb = omip.download_omip_data(as_of=as_of_date)
        print(f"As_of: {as_of_date} has data: {not df_ftb.empty}")
        downloaded_data.append(df_ftb)
    df_ftb = pd.concat(downloaded_data)
    print(df_ftb)
