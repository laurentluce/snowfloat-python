"""Layer tests."""

import json

from mock import patch
import requests
import requests.exceptions

import tests.helper

import snowfloat.geometry
import snowfloat.layer

class LayerTests(tests.helper.Tests):
    """Layer tests."""

    # pylint: disable=C0103
    def setUp(self):
        self.layer = snowfloat.layer.Layer(
            name='test_tag_1',
            uuid='test_layer_1',
            uri='/geo/1/layers/test_layer_1',
            date_created=1,
            date_modified=2,
            num_features=3,
            num_points=6)
        tests.helper.Tests.setUp(self)

    @patch.object(requests, 'get')
    def test_get_features(self, get_mock):
        """Get layer features."""
        self.get_features_test(get_mock, self.layer.get_features)
    
    @patch.object(requests, 'post')
    def test_add_features(self, post_mock):
        """Add layer features."""
        post_mock.__name__ = 'post'
        self.add_features_helper(post_mock, self.layer.add_features,
            self.features)
        self.assertEqual(self.layer.num_features, 10)
        self.assertEqual(self.layer.num_points, 34)

    @patch.object(requests, 'delete')
    def test_delete_features(self, delete_mock):
        """Delete layer features."""
        delete_mock.__name__ = 'delete'
        self.delete_features_helper(delete_mock,
            self.layer.delete_features, field_ts_gte=1, field_ts_lte=10,
            date_created_lte='2002-12-25 00:00:00-00:00')
        self.assertEqual(self.layer.num_features, 1)
        self.assertEqual(self.layer.num_points, 5)
    
    @patch.object(requests, 'delete')
    def test_delete_feature(self, delete_mock):
        """Delete layer feature."""
        delete_mock.__name__ = 'delete'
        self.delete_feature_helper(delete_mock,
            self.layer.delete_feature, 'test_feature_1')

    @patch.object(requests, 'put')
    def test_update(self, put_mock):
        """Update layer."""
        tests.helper.set_method_mock(put_mock, 'put', 200, {})
        self.layer.update(name='test_tag')
        put_mock.assert_called_with(
            '%s/geo/1/layers/test_layer_1' % (tests.helper.URL_PREFIX),
            headers=tests.helper.get_request_body_headers(),
            data=json.dumps({'name': 'test_tag'}),
            params={},
            timeout=10,
            verify=False)
        self.assertEqual(self.layer.name, 'test_tag')

    @patch.object(requests, 'delete')
    def test_delete(self, delete_mock):
        """Delete layer."""
        tests.helper.set_method_mock(delete_mock, 'delete', 200, {})
        self.layer.delete()
        tests.helper.method_mock_assert_called_with(delete_mock,
            '/geo/1/layers/test_layer_1')
