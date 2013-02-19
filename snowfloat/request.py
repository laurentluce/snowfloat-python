import json
import time

import requests

import snowfloat.auth
import snowfloat.errors
import snowfloat.settings

def get(uri, params={}, headers=None):
    while True:
        r = send(requests.get, uri, params=params, headers=headers)
        yield r
        if isinstance(r, dict):
            if 'next_page_uri' in r:
                uri = r['next_page_uri']
                if not uri:
                    break
                # we don't want duplicate params as they are already
                # part of the URI.
                params = {}
                continue
        break

def put(uri, data, headers=None, format_func=None):
    s = 0
    while True:
        d = data[s:s+snowfloat.settings.HTTP_PUT_BATCH_SIZE]
        if d:
            if format_func:
                d = format_func(d)
            r = send(requests.put, uri, data=json.dumps(d), headers=headers)
            yield r
            s += snowfloat.settings.HTTP_PUT_BATCH_SIZE
        else:
            break

def post(uri, data, headers=None):
    r = send(requests.post, uri, data=json.dumps(data), headers=headers)
    return r

def delete(uri, params={}, headers=None):
    r = send(requests.delete, uri, params=params, headers=headers)
    return r

def send(method, uri, params={}, data={}, headers=None):
    h = {'X-Session-ID': snowfloat.auth.session_id}
    if headers:
        h.update(headers)
    
    port = int(snowfloat.settings.HOST.split(':')[1])
    if port != 443:
        scheme = 'http'
    else:
        scheme = 'https'
    
    url = '%s://%s%s' % (scheme, snowfloat.settings.HOST, uri)
    retries = snowfloat.settings.HTTP_RETRIES
    while retries:
        try:
            r = method(url, params=params, data=data, headers=h,
                timeout=snowfloat.settings.HTTP_TIMEOUT, verify=False)
            if r.status_code == 200:
                return r.json()
            elif r.status_code in (400, 403, 404):
                break
        except Exception, e:
            print str(e)
            pass
        time.sleep(snowfloat.settings.HTTP_RETRY_INTERVAL)
        retries -= 1
   
    try:
        status = r.status_code
        content = r.json()
        code = content['code']
        message = content['message']
        more = content['more']
    except (NameError, ValueError):
        status = None
        code = None
        message = r.text
        more = None

    raise snowfloat.errors.RequestError(status, code, message, more)
