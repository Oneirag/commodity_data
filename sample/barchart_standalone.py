"""
This is an example of downloading barchart data to a dataframe, without using downloaders or writing to any
database (No need to use ong_tsdb)
"""
import pandas as pd
from commodity_data.downloaders.barchart import BarchartData

if __name__ == '__main__':
    barchart = BarchartData()
    # Downloads ethereum price. DataFrame has symbol, as_of, open, high, low, close, volume  and oi
    print(df := barchart.download("^ETHUSD", pd.Timestamp(2024, 1, 1)))
    # Downloads eua dec27 price. DataFrame has symbol, as_of, open, high, low, close, volume  and oi
    print(df := barchart.download("CKZ27", pd.Timestamp(2024, 1, 1)))
