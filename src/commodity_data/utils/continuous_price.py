import pandas as pd

from .dfmi import filter_dfmi_columns, update_dfmi_index


def roll_price(price_offset_1: pd.Series, price_offset_2: pd.Series, maturity: pd.Series) -> pd.Series:
    """Returns a new Series with the rolled price. Prices should be in ascending order. Maturity is the maturity of first price offset"""
    adjust = pd.Series(0.0, index=price_offset_1.index)
    # Add ffill so in case lack of prices in offset 1 up to 5 previous days are used
    prev_price_target = price_offset_1.ffill(limit=5).shift()
    prev_price_other = price_offset_2.shift()
    
    roll_day = maturity != maturity.shift()
    roll_cond = roll_day & prev_price_target.notna() & prev_price_other.notna()
    
    adjust.loc[roll_cond] = prev_price_target.loc[roll_cond] - prev_price_other.loc[roll_cond]
    
    retval = adjust.cumsum() + price_offset_1
    
    return retval


def continuous_price(df: pd.DataFrame,
                     target_offset: int = 1,
                     date_col: str = "dat_pricedate",
                     price_col: str = "qua_price",
                     offset_col: str = "offset",
                     maturity_col: str = "dat_maturity",
                     index_col: str = "cod_priceindex",
                     excluded_prods: list[str] = None) -> pd.DataFrame:
    """
    Devuelve un DataFrame con el precio continuado del contrato
    especificado por `target_offset`.

    Parámetros
    ----------
    df : pd.DataFrame
        Tabla tal cual la has leído del CSV.
    target_offset : int, default 1
        Offset cuyo precio continuado queremos calcular.
    date_col, price_col, offset_col, maturity_col : str
        Nombres de las columnas del CSV.
    index_col : str, default cod_price_index
        Columna que identifican al producto (ej.: 'cod_priceindex').
        Si es None se usan todas las columnas que no estén en las anteriores.

    Retorna
    -------
    pd.DataFrame
        Con columnas: `date_col`, `index_cols`, `cont_price_offset{target_offset}`.
    """
    excluded_prods = excluded_prods or []
    # --------- 1. Preparar el set ----------
    index_cols = [index_col]
    # if index_cols is None:
    #     index_cols = [c for c in df.columns
    #                   if c not in {date_col, price_col, offset_col, maturity_col}]

    # Asegúrate de que la fecha sea datetime
    df[date_col] = pd.to_datetime(df[date_col])
    df[maturity_col] = pd.to_datetime(df[maturity_col])

    # Ordenamos para que el shift sea correcto
    df = df.sort_values(index_cols + [date_col, offset_col]).reset_index(drop=True)

    # --------- 2. Pivot (precios por offset) ----------
    df_prices = df.pivot_table(index=index_cols + [date_col],
                               columns=offset_col,
                               values=price_col)
    df_prices.columns = [f"price_{int(col)}" for col in df_prices.columns]
    df_prices = df_prices.reset_index()

    # --------- 3. Maturity del contrato que nos interesa ----------
    df_maturity_target = (df[df[offset_col] == target_offset]
                          .rename(columns={maturity_col: f"{maturity_col}_{target_offset}"})
                          .loc[:, index_cols + [date_col, f"{maturity_col}_{target_offset}"]])

    df_prices = pd.merge(df_prices, df_maturity_target,
                         on=index_cols + [date_col], how="left")

    # Si el offset es 1 o 2 (el caso típico)
    other_offset = 3 - target_offset            # 1 <-> 2

    # --------- 4. Calcular roll ------
    results = []

    for (prod), grp in df_prices.groupby(index_cols):
        if prod[0] in excluded_prods:
            grp["cum_adj"] = 0      # No rolling
        else:
            grp = grp.sort_values(date_col).reset_index(drop=True)

            # precios de hoy del contrato objetivo y del otro
            grp["prev_price_target"] = grp[f"price_{target_offset}"].shift()
            grp["prev_price_other"]  = grp[f"price_{other_offset}"].shift()

            # Día en que el maturity de nuestro offset cambió
            grp["roll_day"] = (grp[f"{maturity_col}_{target_offset}"] !=
                            grp[f"{maturity_col}_{target_offset}"].shift())

            # Ajuste de roll solo en los días de roll
            grp["roll_adj"] = 0.0
            roll_cond = grp["roll_day"] & grp["prev_price_target"].notna() & grp["prev_price_other"].notna()
            grp.loc[roll_cond, "roll_adj"] = (grp.loc[roll_cond, "prev_price_target"] -
                                            grp.loc[roll_cond, "prev_price_other"])

            # Acumulamos los ajustes
            grp["cum_adj"] = grp["roll_adj"].cumsum()

        # Precio continuado que queremos exponer
        grp["cont_price"] = grp[f"price_{target_offset}"] + grp["cum_adj"]

        out = grp[[date_col, *index_cols, "cont_price", "dat_maturity_1", "price_1"]].rename(
            columns={
                "cont_price": f"cont_price_offset_{target_offset}",
                "dat_maturity_1": "dat_maturity",
                "price_1": f"qua_price_offset_{target_offset}"
                    }
            )
        results.append(out)

    return pd.concat(results, ignore_index=True)


def roll_dfmi(dfmi, level_price="close", rolled_type: str = "continuous"):
    """Adds roll to all contracts in given dataframe multiindex. Assumes there is a level called offset"""
    price_df = filter_dfmi_columns(dfmi, type=level_price)
    index_offset = dfmi.columns.names.index("offset")
    for price_col in price_df.columns:
        next_price_col = update_dfmi_index(dfmi, price_col, offset = price_col[index_offset] + 1)
        price_offset_1 = dfmi.loc[:, price_col]
        if next_price_col in price_df.columns:
            rolled = roll_price(price_offset_1, dfmi.loc[:, next_price_col],
                                dfmi.loc[:, update_dfmi_index(dfmi, next_price_col, type="maturity")])
        else:
            # If no future price could be found, returns original price as rolled price
            rolled = price_offset_1
        
        dfmi.loc[:, update_dfmi_index(dfmi, price_col, type=rolled_type)] = rolled            


