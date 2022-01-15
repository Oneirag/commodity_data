"""
Global configurations
"""
from ong_utils import OngConfig, create_pool_manager

__cfg_cdty = OngConfig("commodity_data")
config = __cfg_cdty.config
logger = __cfg_cdty.logger
http = create_pool_manager()

