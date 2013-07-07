"""Client global settings."""

HOST = 'api.snowfloat.com:443'
HTTP_TIMEOUT = 10
HTTP_RETRIES = 3
HTTP_RETRY_INTERVAL = 5

API_KEY = 'IY3487E2J6ZHFOW5A7P5'
API_PRIVATE_KEY = ''

try:
    from settings_prod import *
except ImportError:
    try:
        from settings_dev import *
    except ImportError:
        pass
