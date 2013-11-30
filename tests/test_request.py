"""Request tests."""
import json
import tempfile
import unittest

from mock import Mock, patch
import requests
import requests.exceptions

import tests.helper

import snowfloat.errors
import snowfloat.geometry
import snowfloat.settings

class RequestTests(unittest.TestCase):
    """Request tests."""

    # pylint: disable=C0103
    def setUp(self):
        snowfloat.settings.HOST = 'api.snowfloat.com:443'
    
    def test_get_hmac_sha(self):
        """Get message HMAC-SHA signature."""
        msg = 'test_msg'
        private_key = 'test_private_key'
        res = snowfloat.request._get_hmac_sha(msg, private_key)
        self.assertEqual(res, 'w5YfAjs+VUh79G1jYgHZFWLA4w9W+MNDRX/9z8kFJKY=')

    def test_get_sha_file(self):
        """Get file SHA checksum."""
        tfile = tempfile.NamedTemporaryFile(delete=False)
        tfile.write(''.join('a' for e in xrange(10000)))
        tfile.close()
        tfile = open(tfile.name)
        res = snowfloat.request._get_sha(tfile)
        self.assertEqual(res, 'J90fYbhntqD26dikHEMjHeUhB+U65CTej4R7gh20txE=')
        tfile.close()

    def test_get_sha_str(self):
        """Get string SHA checksum."""
        res = snowfloat.request._get_sha('test_message')
        self.assertEqual(res, 'O3SR3AFqwaCy4CNyQCyGH6+kWSlAh+fL4J9wTVgtkx8=')

    def test_format_url_https(self):
        """Format full path https URL."""
        res = snowfloat.request._format_url('/test_uri')
        self.assertEqual(res, 'https://api.snowfloat.com:443/test_uri')

    def test_format_url_http(self):
        """Format full path http URL."""
        snowfloat.settings.HOST = 'api.snowfloat.com:80'
        res = snowfloat.request._format_url('/test_uri')
        self.assertEqual(res, 'http://api.snowfloat.com:80/test_uri')

    def test_format_params_query_distance(self):
        """Format params for query with distance."""
        point = snowfloat.geometry.Point([1, 2, 3])
        kwargs = {'query': 'test_query',
                  'geometry': point,
                  'distance': 1}
        exclude = ('geometry', 'distance')
        res = snowfloat.request.format_params(kwargs, exclude)
        geojson = {'type': 'Point',
                   'coordinates': [1, 2, 3],
                   'properties': {'distance': 1}}
        self.assertDictEqual(res,
            {'geometry__test_query': json.dumps(geojson)})

    def test_format_params_query_distance_missing(self):
        """Format params for query with distance missing."""
        point = snowfloat.geometry.Point([1, 2, 3])
        kwargs = {'query': 'test_query',
                  'geometry': point}
        exclude = ('geometry', 'distance')
        res = snowfloat.request.format_params(kwargs, exclude)
        geojson = {'type': 'Point',
                   'coordinates': [1, 2, 3],
                   'properties': {'distance': None}}
        self.assertDictEqual(res,
            {'geometry__test_query': json.dumps(geojson)})

    def test_format_params_query_slice_start(self):
        """Format params for query with query_slice single item."""
        kwargs = {'query_slice': (1,)}
        res = snowfloat.request.format_params(kwargs)
        self.assertDictEqual(res,
            {'slice_start': 1})

    def test_format_params_query_slice_stop(self):
        """Format params for query with query_slice two items."""
        kwargs = {'query_slice': (1, 2)}
        res = snowfloat.request.format_params(kwargs)
        self.assertDictEqual(res,
            {'slice_start': 1,
             'slice_end': 2})

    @patch.object(snowfloat.request, '_get_headers')
    @patch.object(requests, 'get')
    def test_send_get(self, get_mock, get_headers_mock):
        """Send function with get method."""
        get_mock.__name__ = 'get'
        m = Mock()
        m.status_code = 200
        m.json.return_value = 'test_response'
        get_mock.return_value = m
        get_headers_mock.return_value = {'header_1': 'test_header_1'}
        uri = '/test_uri'
        params = {'param': 'test_param'}
        headers = {'header_2': 'test_header_2'}
        res = snowfloat.request.send(get_mock, uri, params=params,
            headers=headers)
        self.assertEqual(res, 'test_response')
        get_mock.assert_called_with(
            '%s/test_uri' % (tests.helper.URL_PREFIX),
            headers={'header_1': 'test_header_1',
                     'header_2': 'test_header_2'},
            params={'param': 'test_param'},
            data={},
            timeout=10,
            verify=False)

    @patch.object(requests, 'get')
    def test_get_headers(self, get_mock):
        """Test _get_headers."""
        get_mock.__name__ = 'get'
        uri = '/test_uri'
        request_params = {'param': 'test_param'}
        request_data = {}
        headers = snowfloat.request._get_headers(get_mock, uri, request_data,
            request_params)
        self.assertEqual(headers['Authorization'],
                'GEO QWE948OCAYX16G1XVGJM:'\
                'azh8WQmu8IwP15TFbWafei1s5VWZGAkMzNivHXBlqD4=')

    @patch.object(requests, 'get')
    def test_get_headers_sharing(self, get_mock):
        """Test _get_headers with user sharing keys."""
        get_mock.__name__ = 'get'
        uri = '/test_uri'
        request_params = {'param': 'test_param'}
        request_data = {}
        snowfloat.settings.USER_API_KEY_ID = 'test_user_api_key_id'
        snowfloat.settings.USER_API_SHARING_KEY = 'test_user_api_sharing_key'
        headers = snowfloat.request._get_headers(get_mock, uri, request_data,
            request_params)
        self.assertEqual(headers['X-User-API-Key-ID'],
            'test_user_api_key_id')
        self.assertEqual(headers['X-User-API-Sharing-Key'],
            'test_user_api_sharing_key')
        snowfloat.settings.USER_API_KEY_ID = ''
        snowfloat.settings.USER_API_SHARING_KEY = ''


class RequestErrorTests(unittest.TestCase):
    """Request error tests."""
    def test_request_error(self):
        """Request error test."""
        req = snowfloat.errors.RequestError(500, 1, 'test_message',
            'test_more')
        self.assertEqual(str(req),
            'RequestError: status=500, code=1, message=test_message, ' \
            'more=test_more')
        req = eval('snowfloat.errors.' + repr(req))
        self.assertTrue(isinstance(req, snowfloat.errors.RequestError))
        self.assertEqual(req.status, 500)
        self.assertEqual(req.code, 1)
        self.assertEqual(req.message, 'test_message')
        self.assertEqual(req.more, 'test_more')



