"""
Configuration for a simple commodity series and instruments
Examples:
    market commodity   instrument  Area            Product     Maturity
    Omip    Power       Base/Peak   Es/Pt/De/Fr     M/W/Y/Q     ---
    Omip    Emissions   EUA         Eu              Y           ---
    Omip    Gas         Base        Es              MQY         ---
"""
import datetime
from dataclasses import field
from enum import Enum

from marshmallow.validate import OneOf
from marshmallow_dataclass import dataclass

valid_product = "YMQD"

df_index_columns = ["market",  # market from which data is downloaded (Omip, ICE...)
                    "commodity",  # Generic name of commodity (Power, Gas, CO2....)
                    "instrument",  # BL (baseload)/PK (peak load), EUA...
                    "area",  # Country
                    "product",  # D/W/M/Q/Y for calendar day/week/month/quarter/year
                    "offset",  # Number of calendar products of interval from as_of date till maturity
                    "type",     # From TypeColumn
                    ]


class TypeColumn(str, Enum):
    close = "close"             # Original settlement price
    adj_close = "adj_close"     # Adjusted settlement price (taking into account product rolling)


@dataclass
class CommodityCfg:
    commodity: str
    instrument: str
    area: str = ""


@dataclass
class _OmipDownloadConfig:
    instrument: str
    zone: str
    start_t: datetime.date
    product: str


@dataclass
class _BarchartDownloadConfig:
    symbol: str
    product: str = field(default="D", metadata={"validate": OneOf(valid_product)})
    expiry: datetime.date = field(default=None)


@dataclass
class BarchartConfig:
    commodity_cfg: CommodityCfg
    download_cfg: _BarchartDownloadConfig

    def id(self) -> str:
        return self.download_cfg.symbol


@dataclass
class OmipConfig:
    commodity_cfg: CommodityCfg
    download_cfg: _OmipDownloadConfig

    def id(self) -> str:
        return ",".join([self.download_cfg.instrument, self.download_cfg.product, self.download_cfg.zone])
