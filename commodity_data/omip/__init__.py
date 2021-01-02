import pandas as pd
from commodity_data.series_config import CommodityCfg


# Configuration of Omip markets for downloading
class OmipConfig:

    eex_omip_start = pd.Timestamp(2016, 5, 13)
    omip_start = pd.Timestamp(2006, 1, 1)       # There are just quarters before that date...boring...
    mibgas_start = pd.Timestamp(2017, 11, 24)
    commodity_config = {
        # Power baseloads
        CommodityCfg("Power", "BL", "ES"): {
            "download_config": {"instrument": "FTB", "product": "EL", "zone": "ES"},
            "start_t": omip_start,
        },
        CommodityCfg("Power", "BL", "DE"): {
            "download_config": {"instrument": "FDB", "product": "EL", "zone": "DE"},
            "start_t": eex_omip_start,
        },
        CommodityCfg("Power", "BL", "FR"): {
            "download_config": {"instrument": "FFB", "product": "EL", "zone": "FR"},
            "start_t": eex_omip_start,
        },
        # Gas
        CommodityCfg("Gas", "BL", "ES"): {
            "download_config": {"instrument": "FGE", "zone": "ES", "product": "NG"},
            "start_t": mibgas_start,
        },
    }
