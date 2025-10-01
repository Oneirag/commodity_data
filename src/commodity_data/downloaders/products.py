"""
functions related to products:
    - List of valid products
    - Functions to calculate time offsets between as_of dates and the maturity of a product (YMQDW)
    - Function to format a date into a standard delivery month (e.g. 2024-12-1 -> Z24)
"""
from datetime import datetime

import pandas as pd
from pandas.core.indexes.accessors import DatetimeProperties

# These are the products marked as valid
valid_product = [
    "Y", "M", "Q", "D", "W"
]

delivery_months = {
    "January": "F",
    "February": "G",
    "March": "H",
    "April": "J",
    "May": "K",
    "June": "M",
    "July": "N",
    "August": "Q",
    "September": "U",
    "October": "V",
    "November": "X",
    "December": "Z",
    1: "F",
    2: "G",
    3: "H",
    4: "J",
    5: "K",
    6: "M",
    7: "N",
    8: "Q",
    9: "U",
    10: "V",
    11: "X",
    12: "Z",

}


def pd_date_offset(as_of_series: DatetimeProperties, maturity: pd.Timestamp, product: str) -> list:
    """Calculates standard offset between two dates. as_of_series must be df['date_field'].dt.
    Returns a list of offset values """
    data = [date_offset(as_of, maturity, product) for as_of in as_of_series.date]
    return list(data)


def date_offset(as_of: pd.Timestamp, maturity: pd.Timestamp, product: str) -> int:
    """Returns difference between two dates in period (YMQDW)"""
    if product == "Y":
        return maturity.year - as_of.year
    elif product == "M":
        return (maturity.year - as_of.year) * 12 + maturity.month - as_of.month
    elif product == "Q":
        return (maturity.year - as_of.year) * 4 + ((maturity.month - 1) // 3 + 1) - (
                (as_of.month - 1) // 3 + 1)
    elif product == "D":
        return (maturity - as_of).days
    elif product == "W":
        # In calendar weeks, starting on monday
        start_week_monday = as_of - pd.offsets.Day(as_of.weekday())
        return (maturity - start_week_monday).days // 7
    else:
        raise ValueError(f"Invalid product {product}")


def to_standard_delivery_month(maturity: datetime) -> str:
    """Returns a date converted to standard deliveries. E.g: for datetime(2025,12,3) returns 'Z25'"""
    return delivery_months[int(maturity.strftime("%m"))] + maturity.strftime("%y")
