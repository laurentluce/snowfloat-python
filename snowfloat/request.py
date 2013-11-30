"""HTTP requests to server."""

import base64
import email.utils
import hashlib
import hmac
import json
import time
import urllib

import requests
import requests.exceptions

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

        data (str): Request body data.

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

    request_headers = _get_headers(method, uri, request_data, request_params)
    
    if headers:
        request_headers.update(headers)

    url = _format_url(uri)

    message = None
    retries = snowfloat.settings.HTTP_RETRIES
    timeout = snowfloat.settings.HTTP_TIMEOUT
    while retries:
        try:
            res = method(url, params=request_params, data=request_data,
                headers=request_headers,
                timeout=timeout, verify=False)
            if res.status_code == 200:
                return res.json()
            elif res.status_code in (400, 401, 403, 404, 413):
                break
        except requests.exceptions.RequestException, exception:
            message = str(exception)
            if 'timeout' in message:
                timeout *= 2
            res = None

        time.sleep(snowfloat.settings.HTTP_RETRY_INTERVAL)
        retries -= 1

    raise_request_error(res, message)

def raise_request_error(res, message):
    """Raise a RequestError exception.

    Args:
        res (Requests response): HTTP response.

        message (str): Request error message.

    Raises:
        snowfloat.errors.RequestError
    """
    status = None
    code = None
    more = None
    try:
        status = res.status_code
        content = res.json()
        code = content['code']
        message = content['message']
        more = content['more']
    except (NameError, ValueError, AttributeError):
        pass 

    raise snowfloat.errors.RequestError(status, code, message, more)

def _get_hmac_sha(msg, private_key):
    """Return HMAC-SHA of the message using the private key.

    Args:
        msg (str): Message to sign.
        
        private_key (str): Key to use to sign.

    Returns:

        str: base64 encoded signature.
    """
    return base64.b64encode(hmac.new(
        private_key, msg=msg, digestmod=hashlib.sha256).digest())

def _get_sha(request_data):
    """Return request data SHA-256 checksum.

    Args:
        request_data (str or file): Request data.

    Returns:
        str: base64 encoded checksum.
    """
    if isinstance(request_data, file):
        sha = hashlib.sha256()
        while True:
            chunk = request_data.read(8192)
            if chunk:
                sha.update(chunk)
            else:
                request_data.seek(0)
                break
    else:
        sha = hashlib.sha256(request_data)

    return base64.b64encode(sha.digest())

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

def _get_headers(method, uri, request_data, request_params):
    """Return dictionary of headers based on request body and parameters.

    Args:

        method (str): HTTP method.

        uri (str): HTTP URI.

        request_data (str or file): Request body.
        
        request_params (dict): Request parameters.

    Returns:

        dict: Request headers.
    """
    verb = method.__name__.upper()
    if verb in ('PUT', 'POST'):
        content_sha = _get_sha(request_data)
        
        if isinstance(request_data, file):
            content_type = 'application/octet-stream'
        else:
            content_type = 'application/json'
    else:
        content_sha = ''
        content_type = ''

    date = email.utils.formatdate(usegmt=True)

    if request_params:
        full_uri = '%s?%s' % (uri, urllib.urlencode(request_params))
    else:
        full_uri = uri

    msg = '%s\n%s\n%s\n%s\n%s' % (
        verb, content_sha, content_type, date, full_uri)

    request_headers = {'Authorization': 'GEO %s:%s' % (
        snowfloat.settings.API_KEY_ID,
        _get_hmac_sha(msg, snowfloat.settings.API_SECRET_KEY))}
    
    if verb in ('PUT', 'POST'):
        request_headers['Content-Sha'] = content_sha
        request_headers['Content-Type'] = content_type
    request_headers['Date'] = date

    if snowfloat.settings.USER_API_KEY_ID:
        request_headers['X-User-API-Key-ID'] = \
            snowfloat.settings.USER_API_KEY_ID 

    if snowfloat.settings.USER_API_SHARING_KEY:
        request_headers['X-User-API-Sharing-Key'] = \
            snowfloat.settings.USER_API_SHARING_KEY 

    return request_headers

def format_params(kwargs, exclude=None):
    """Return formatted parameters ready to be consumed by the API.

    Args:

        kwargs (dict): Dictionary of parameters.

        exclude (tuple): Tuple of parameters to exclude.

    Returns:

        dict: Dictionary of formatted parameters.
    """    
    if not exclude:
        exclude = ()
    params = {}
    for key, val in kwargs.items():
        if key == 'query':
            try:
                distance = kwargs['distance']
            except KeyError:
                distance = None
            geojson = {'type': kwargs['geometry'].geometry_type,
                       'coordinates': kwargs['geometry'].coordinates,
                       'properties': {
                           'distance': distance}
                      }
            params['geometry__%s' % (val,)] = json.dumps(geojson)
        elif key == 'order_by':
            params[key] = ','.join(val)
        elif key == 'query_slice':
            if len(val) == 2:
                params['slice_end'] = val[1]
            params['slice_start'] = val[0]
        elif (not key.startswith('spatial_')
                and not key in exclude):
            key = key.replace('layer_', 'layer__')
            prefix = key[:key.rindex('_')]
            suffix = key[key.rindex('_')+1:]
            params[prefix + '__' + suffix] = val

    return params

