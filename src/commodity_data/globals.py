"""
Common variables: configuration, logger, etc. They cannot be in __init__ to avoid cross-references
It includes the set_proxy_user_password method to update values for proxy in the keyring storage
and a CustomLocale context manager to set temporarily locale
"""
from ong_utils import OngConfig, create_pool_manager
import locale

__cfg_cdty = OngConfig("commodity_data")
config = __cfg_cdty.config
logger = __cfg_cdty.logger
http = create_pool_manager()
get_password = __cfg_cdty.get_password
set_password = __cfg_cdty.set_password


class CustomLocale:
    """Sets temporarily a custom locale, and returns back to original locale at exit. As a default, sets locale to
    en_US"""
    def __init__(self, category = locale.LC_TIME, new_locale = 'en_US.UTF-8'):
        self.category = category
        self.new_locale = new_locale
        self.original_locale = None

    def __enter__(self):
        self.original_locale = locale.getlocale(self.category)
        locale.setlocale(self.category, self.new_locale)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restaurar el locale original
        locale.setlocale(self.category, self.original_locale)


def set_proxy_user_password():
    # Sets passwords for proxy
    for service_name in ("service_name_proxy", "service_name_google_auth"):
        print(f"Setting password for {service_name}")
        set_password(service_name, "proxy_username")
        print(get_password(service_name, "proxy_username"))


if __name__ == '__main__':
    set_proxy_user_password()
