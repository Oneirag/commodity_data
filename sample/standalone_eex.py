"""
Example to download EEX data standalone, without writing to any database
In the case of EEX, data has to be downloaded per day, product and delivery
"""
import matplotlib.pyplot as plt
import pandas as pd

from commodity_data.downloaders.eex import EEXData

if __name__ == '__main__':

    as_of = pd.Timestamp("2024-03-18")      # Sample day for downloads

    # Initialize object. In the first execution, creates a cache file with all data of EEX, that takes
    # around 30 seconds
    eex = EEXData()

    """
    Finding symbols in EEX (if you don't know them!)
    """
    # Get all configuration of EEX, and analyze it
    all_config = eex.get_eex_config_df()
    print(all_config)
    for column in ("market", "delivery", "type"):
        print(f"Different Values for {column}")
        print(all_config[column].unique())

    # Get a specific symbol
    # This is spanish Baseload power, day ahead
    symbol_spanish_power_day = eex.get_eex_symbol("Spanish", delivery="Day", type_="base")
    print(symbol_spanish_power_day)     # This is ['/E.FE_DAILY']
    # If you want to get more info on the different symbols:
    df_spanish_power = eex.get_eex_config_df(market="Spanish")
    print(df_spanish_power)         # Will get a DataFrame with market, delivery, code and type

    """
    Downloading daily symbols from EEX
    """
    # Look for a specific symbol to download, and download it
    # Example: Spanish Baseload power, day ahead
    symbol_spanish_power_day = eex.get_eex_symbol("Spanish", delivery="Day")
    df_spanish_power_day = eex.download_symbol_chain_table(symbol=symbol_spanish_power_day, date=as_of)
    print(df_spanish_power_day)

    # Download data between two dates. It has to be downloaded day by day
    liquid_milk_code = eex.get_eex_symbol("Liquid Milk")
    downloaded_data = list()
    for as_of_date in pd.bdate_range(as_of - pd.offsets.BDay(5), as_of):
        df_milk = eex.download_symbol_chain_table(symbol=liquid_milk_code, date=as_of_date)
        print(f"As_of: {as_of_date} has data: {not df_milk.empty}")
        downloaded_data.append(df_milk)
    df_milk = pd.concat(downloaded_data)
    print(df_milk)

    """
    Download all data for a specific market price
    The eex symbol is not enough, you need the price_symbol, that has the market symbol and a specific delivery
    code. 
    """
    market_code = eex.get_eex_symbol("Liquid Milk", delivery="Month")[0]
    maturity = pd.Timestamp.today().normalize() + pd.offsets.BMonthBegin(1)
    # Option 1: Get the price code downloading data for a simple day and parsing results
    price_code1 = eex.get_eex_price_symbol(market_code, maturity)
    # Option 2. Built it using month future code plus two-digit year
    price_code2 = eex.get_eex_price_symbol(market_code, maturity, no_download=True)
    assert price_code1 == price_code2

    for market in "Liquid milk", "Processing Potato", "Skimmed Milk Powder":
        market_code = eex.get_eex_symbol(market, delivery="Month")[0]
        price_code = eex.get_eex_price_symbol(market_code, maturity)
        if not price_code:
            print(f"could not find price code for {market_code} {maturity}")
            continue
        df = eex.download_price_symbol_history(price_code, since="all")
        print(df)
        df.set_index("tradedatetimegmt", inplace=True)
        df.close.plot(title=market)
        plt.show()
