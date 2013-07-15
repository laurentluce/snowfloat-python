"""Client global settings."""

HOST = 'api.snowfloat.com:443'
HTTP_TIMEOUT = 10
HTTP_RETRIES = 3
HTTP_RETRY_INTERVAL = 5

API_KEY = ''
API_PRIVATE_KEY = ''

try:
    # pylint: disable=F0401
    from settings_prod import *
except ImportError:
    try:
        # pylint: disable=F0401
        from settings_dev import *
    except ImportError:
        pass
