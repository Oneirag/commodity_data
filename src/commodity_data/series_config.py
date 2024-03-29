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

valid_product = "YMQDW"

df_index_columns = ["market",  # market from which data is downloaded (Omip, ICE...)
                    "commodity",  # Generic name of commodity (Power, Gas, CO2....)
                    "instrument",  # BL (baseload)/PK (peak load), EUA...
                    "area",  # Country
                    "product",  # D/W/M/Q/Y for calendar day/week/month/quarter/year
                    "offset",  # Number of calendar products of interval from as_of date till maturity
                    "type",  # From TypeColumn
                    ]


class TypeColumn(str, Enum):
    close = "close"  # Original settlement price
    adj_close = "adj_close"  # Adjusted settlement price (taking into account product rolling)


@dataclass
class CommodityCfg:
    commodity: str
    instrument: str
    area: str = ""


class _BaseDownloadConfig:

    def meets(self, filter: dict) -> bool:
        """Returns True if this config meets the values of other config. Checks that all non empty fields are equal"""
        check_fields = self.__dict__.keys()
        if difference := (set(filter) - set(check_fields)):
            raise TypeError(f"Filter contains invalid fields: {difference}")
        return all(
            getattr(self, field_name) == (filter.get(field_name, None) or getattr(self, field_name))
            for field_name in check_fields
        )


@dataclass
class _OmipDownloadConfig(_BaseDownloadConfig):
    instrument: str
    zone: str
    start_t: datetime.date
    product: str


@dataclass
class _BarchartDownloadConfig(_BaseDownloadConfig):
    symbol: str
    product: str = field(default="D", metadata={"validate": OneOf(valid_product)})
    expiry: datetime.date = field(default=None)


@dataclass
class BarchartConfig:
    commodity_cfg: CommodityCfg
    download_cfg: _BarchartDownloadConfig

    def id(self) -> str:
        return self.download_cfg.symbol

    def to_dict(self):
        """Returns a dictionary of the elements of config that are also defined in the columns"""
        return {k: getattr(self, k) for k in df_index_columns if getattr(self, k, None) is not None}


@dataclass
class OmipConfig:
    commodity_cfg: CommodityCfg
    download_cfg: _OmipDownloadConfig

    def id(self) -> str:
        return ",".join([self.download_cfg.instrument, self.download_cfg.product, self.download_cfg.zone])


@dataclass
class _EEXDownloadConfig(_BaseDownloadConfig):
    instrument: str
    product: str


@dataclass
class EEXConfig:
    commodity_cfg: CommodityCfg
    download_cfg: _EEXDownloadConfig

    def id(self) -> str:
        return self.download_cfg.instrument
