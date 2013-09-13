"""Client global settings."""
import os
import ConfigParser

HOST = 'api.snowfloat.com:443'
HTTP_TIMEOUT = 10
HTTP_RETRIES = 3
HTTP_RETRY_INTERVAL = 5

API_KEY_ID = ''
API_SECRET_KEY = ''

CONFIG = ConfigParser.RawConfigParser()
for loc in (os.curdir, os.path.expanduser("~"), "/etc/snowfloat"):
    try:
        with open(os.path.join(loc, "snowfloat.conf")) as source:
            CONFIG.readfp(source)
            API_KEY_ID = CONFIG.get('snowfloat', 'api_key_id')
            API_SECRET_KEY = CONFIG.get('snowfloat', 'api_secret_key')
            break
    except IOError:
        pass
