"""
Calculates contract size and adds to a pivoted dataframe
"""
from typing import Literal
import pandas as pd
import re
from enum import StrEnum
from .dfmi import update_dfmi_index, filter_dfmi_columns


class Duration(StrEnum):
    """Determines duration in time"""
    hours_year: str = "HY"
    hours_month: str = "HM"
    hours_day: str = "HD"
    hours_quarter: str = "HQ"
    days_year: str = "DY"
    days_month: str = "DM"
    days_quarter: str = "DQ"


def get_period_duration_simple(maturity_date, duration: Duration) -> int:
    """
    Calcula la duración en horas y días de un periodo natural.

    Parameters:
    maturity_date (str or datetime): Fecha de inicio del periodo (siempre día 1)
    duration (str): Duración del periodo ('year', 'quarter', 'month')

    Returns:
    horas o días del periodo, en función de period
    """

    # Convertir la fecha a datetime si es string
    if isinstance(maturity_date, str):
        maturity_date = pd.to_datetime(maturity_date)

    # Calcular la fecha de finalización del periodo
    if duration in (Duration.days_year, Duration.hours_year):
        end_date = maturity_date + pd.DateOffset(years=1)
    elif duration in (Duration.days_quarter, Duration.hours_quarter):
        end_date = maturity_date + pd.DateOffset(months=3)
    elif duration in (Duration.days_month, Duration.hours_month):
        end_date = maturity_date + pd.DateOffset(months=1)
    else:
        raise ValueError("Invalid duration: {duration}")

    # Calcular la diferencia en días
    days = (end_date - maturity_date).dt.days

    # Convertir a horas (falla por los cambios de hora, se puede ignorar)
    hours = days * 24

    retval = days if duration in (Duration.days_year, Duration.days_month, Duration.days_quarter) else hours
    return retval


def set_contract_size(df: pd.DataFrame, price_index_regex: str, 
                      duration: Duration | int,
                      suffix_price="_price", suffix_maturity="_maturity", suffix_size="_size"):

    # Get just the interesting columns
    filtered_df_maturities = df.filter(regex=price_index_regex).filter(regex=f"{suffix_maturity}$")
    for maturity_code in filtered_df_maturities.columns:
        maturity = pd.to_datetime(filtered_df_maturities[maturity_code])
        new_column = maturity_code[:-len(suffix_maturity)] + suffix_size
        if isinstance(duration, Duration):
            contract_size = get_period_duration_simple(maturity, duration=duration)
        else:
            contract_size = duration
        df.loc[:, new_column] = contract_size
            
    
    return df

def size_dfmi(dfmi, size_spec: Duration | int, col_filter: dict = None, size_type="size"):
    """Adds a size column to the multiindex dataframe with the given filter"""
    filtered_df = filter_dfmi_columns(dfmi, **(col_filter or dict()))
    for maturity_col in filtered_df.xs("maturity", level="type", axis=1, drop_level=False).columns:
        size_col = update_dfmi_index(dfmi, maturity_col, type=size_type)
        if isinstance(size_spec, int):
            dfmi.loc[:, size_col] = size_spec 
        else:
            dfmi.loc[:, size_col] = get_period_duration_simple(filtered_df[maturity_col], size_spec)

    

if __name__ == "__main__":
    
    # df = pd.read_csv("all_data.csv")
    df = pd.read_csv("algo_trading.csv")
        
    for price_regex, duration in [
        (".*", 1000),       # By default, 1k size (works for EUA and Brent)
        # Power futures, in hours
        ("^.*Power.*_BL.*_Q_.*", Duration.hours_quarter),
        ("^.*Power.*_BL.*_M_.*", Duration.hours_month),
        ("^.*Power.*_BL.*_Y_.*", Duration.hours_year),
        # Gas Futures also in hours
        ("^.*Gas.*_Q_.*", Duration.hours_quarter),
        ("^.*Gas.*_M_.*", Duration.hours_month),
        ("^.*Gas.*_Y_.*", Duration.hours_year),
        # Henry hub fitures, in 10kMBTU
        ("GAS_HH_US_", 10_000)
        
    ]:
        df = set_contract_size(df=df, 
                               price_index_regex=price_regex,
                               duration=duration)
    
    print(df)
    