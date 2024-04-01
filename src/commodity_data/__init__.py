"""
Global configurations
"""

from commodity_data.commodity_data import CommodityData
from commodity_data.common import get_password, set_password


def set_proxy_user_password():
    # Sets passwords for proxy
    for service_name in ("service_name_proxy", "service_name_google_auth"):
        print(f"Setting password for {service_name}")
        set_password(service_name, "proxy_username")
        print(get_password(service_name, "proxy_username"))


if __name__ == '__main__':
    set_proxy_user_password()
