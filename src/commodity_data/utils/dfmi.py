"""
Some utility functions for working with pandas DataFrames with multiindex columns
"""

def filter_dfmi_columns(df, **kwargs):
    """Allows filtering a multiindex using pairs level_name=value.
    Example: filter_dfmi_columns(df, market="EEX", commodity="Power", type="close")"""
    mask = None
    for k, v in kwargs.items():
        new_mask = df.columns.get_level_values(k) == v
        if mask is None:
            mask = new_mask
        else:
            mask &= new_mask
    if mask is None:
        return df
    else:
        return df.loc[:, mask]
    
def update_dfmi_index(dfmi, index: tuple, **kwargs) -> tuple:
    """Given a dataframe multiindex and an index, return a new index with the levels changed.
    Example: update_dfmi_index(dfmi, ("one", "two", "three"), level2="four") returns ("one", "four", "three")
    """
    new_index = [v for v in index]
    for k, v in kwargs.items():
        pos = dfmi.columns.names.index(k)
        new_index[pos] = v
    return tuple(new_index)   