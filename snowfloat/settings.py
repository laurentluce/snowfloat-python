"""Client global settings."""
import os
import ConfigParser

HOST = 'api.snowfloat.com:443'
HTTP_TIMEOUT = 10
HTTP_RETRIES = 3
HTTP_RETRY_INTERVAL = 5

API_KEY = ''
API_PRIVATE_KEY = ''

CONFIG = ConfigParser.RawConfigParser()
for loc in (os.curdir, os.path.expanduser("~"), "/etc/snowfloat"):
    try:
        with open(os.path.join(loc, "snowfloat.conf")) as source:
            CONFIG.readfp(source)
            API_KEY = CONFIG.get('snowfloat', 'api_key')
            API_PRIVATE_KEY = CONFIG.get('snowfloat', 'api_private_key')
            break
    except IOError:
        pass
