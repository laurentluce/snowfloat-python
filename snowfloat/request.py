"""HTTP requests to server."""

import json
import time

import requests

import snowfloat.auth
import snowfloat.errors
import snowfloat.geometry
import snowfloat.settings

def get(uri, params=None, headers=None):
    """GET from server.

    Args:
        uri (str): Request URI.

    Kwargs:
        params (dict): Request parameters.

        headers (dict): Request headers.

    Returns:
        generator: Yields response.
    """
    request_params = params
    if request_params is None:
        request_params = {}
    while True:
        res = send(requests.get, uri, params=request_params, headers=headers)
        yield res
        if isinstance(res, dict):
            if 'next_page_uri' in res:
                uri = res['next_page_uri']
                if not uri:
                    break
                # we don't want duplicate params as they are already
                # part of the URI.
                request_params = {}
                continue
        break

def post(uri, data, headers=None, format_func=None, serialize=True):
    """POST to server.

    Args:
        uri (str): Request URI.

        data: POST data.

    Kwargs:
        headers (dict): Request headers.

        format_func (function): Format data using this function before sending.

        serialize (bool): JSON-serialize the data to post or not.

    Returns:
        str: Server response.
    """
    data_to_post = data
    if format_func:
        data_to_post = format_func(data_to_post)
    if serialize:
        data_to_post = json.dumps(data_to_post)
    return send(requests.post, uri, data=data_to_post, headers=headers)

def put(uri, data, headers=None):
    """PUT to server.

    Args:
        uri (str): Request URI.

        data: POST data.

    Kwargs:
        headers (dict): Request headers.

    Returns:
        str: Server response.
    """
    return send(requests.put, uri, data=json.dumps(data), headers=headers)

def delete(uri, params=None, headers=None):
    """DELETE on server.

    Args:
        uri (str): Request URI.

    Kwargs:
        params (dict): Request parameters.

        headers (dict): Request headers.

    Returns:
        str: Server response.
    """
    request_params = params
    if request_params is None:
        request_params = {}
    return send(requests.delete, uri, params=request_params, headers=headers)

def send(method, uri, params=None, data=None, headers=None):
    """Send request to server.

    Args:
        method (method): Requests library method to call.

        uri (str): Request URI.

    Kwargs:
        params (dict): Request parameters.

        headers (dict): Request headers.

    Returns:
        str: Server response.
    """
    request_params = params
    if request_params is None:
        request_params = {}

    request_data = data
    if request_data is None:
        request_data = {}

    request_headers = {'X-Session-ID': snowfloat.auth.session_uuid}
    if headers:
        request_headers.update(headers)
   
    url = _format_url(uri)

    retries = snowfloat.settings.HTTP_RETRIES
    while retries:
        try:
            res = method(url, params=request_params, data=request_data,
                headers=request_headers,
                timeout=snowfloat.settings.HTTP_TIMEOUT, verify=False)
            if res.status_code == 200:
                print res.text
                return res.json()
            elif res.status_code in (400, 403, 404, 413):
                break
        except Exception:
            pass
        time.sleep(snowfloat.settings.HTTP_RETRY_INTERVAL)
        retries -= 1
   
    status = None
    code = None
    message = None
    more = None
    try:
        status = res.status_code
        content = res.json()
        code = content['code']
        message = content['message']
        more = content['more']
    except (NameError, ValueError):
        pass 

    raise snowfloat.errors.RequestError(status, code, message, more)

def _format_url(uri):
    """Format URL based on URI and port number.

    Args:
        uri (str): Request URI.

    Returns:
        str: URL.
    """
    port = int(snowfloat.settings.HOST.split(':')[1])
    if port != 443:
        scheme = 'http'
    else:
        scheme = 'https'
    
    url = '%s://%s%s' % (scheme, snowfloat.settings.HOST, uri)

    return url
