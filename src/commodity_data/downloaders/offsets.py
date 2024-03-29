"""
Functions to calculate time offsets between as_of dates and the maturity of a product (YMQDW)
"""
import pandas as pd
from pandas.core.indexes.accessors import DatetimeProperties


def pd_date_offset(as_of_series: DatetimeProperties, maturity: pd.Timestamp, product: str) -> list:
    """Calculates standard offset between two dates. as_of_series must be df['date_field'].dt.
    Returns a list of offset values """
    data = [date_offset(as_of, maturity, product) for as_of in as_of_series.date]
    return list(data)


def date_offset(as_of: pd.Timestamp, maturity: pd.Timestamp, product: str) -> int:
    """Returns difference between two dates in period (YMQD)"""
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
