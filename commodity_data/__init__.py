"""
Global configurations
"""
from ong_utils import OngConfig, create_pool_manager

__cfg_cdty = OngConfig("commodity_data")
config = __cfg_cdty.config
logger = __cfg_cdty.logger
get_password = __cfg_cdty.get_password
http = create_pool_manager()


def set_proxy_user_password():
    # Sets passwords for proxy
    set_password = __cfg_cdty.set_password
    for service_name in ("service_name_proxy", "service_name_google_auth"):
        print(f"Setting password for {service_name}")
        set_password(service_name, "proxy_username")
        print(get_password(service_name, "proxy_username"))


if __name__ == '__main__':
    set_proxy_user_password()

