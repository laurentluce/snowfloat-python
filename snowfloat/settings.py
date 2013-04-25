HOST = 'api.snowfloat.com:443'
HTTP_TIMEOUT = 10
HTTP_RETRIES = 3
HTTP_RETRY_INTERVAL = 5

try:
    from snowfloat.settings_prod import *
except ImportError:
    try:
        from snowfloat.settings_dev import *
    except ImportError:
        pass
