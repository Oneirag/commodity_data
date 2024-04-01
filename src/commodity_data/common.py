"""
Common variables: configuration, logger, etc. They cannot be in __init__
to avoid cross-references
"""
from ong_utils import OngConfig, create_pool_manager

__cfg_cdty = OngConfig("commodity_data")
config = __cfg_cdty.config
logger = __cfg_cdty.logger
http = create_pool_manager()
get_password = __cfg_cdty.get_password
set_password = __cfg_cdty.set_password
