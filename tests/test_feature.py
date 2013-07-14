"""Features tests."""

import json

from mock import patch
import requests

import tests.helper

import snowfloat.geometry

class FeaturesTests(tests.helper.Tests):
    """Features tests."""
    point = snowfloat.geometry.Point(coordinates=[1, 2, 3])
    fields = {'ts': 1, 'tag': 'test_tag'}
    uri = '/geo/1/layers/test_layer_1/features/test_feature_1'
    feature = snowfloat.feature.Feature(point, fields=fields, uri=uri)

    @patch.object(requests, 'put')
    def test_update(self, put_mock):
        """Update feature."""
        tests.helper.set_method_mock(put_mock, 'put', 200, {})
        fields = {'ts': 2, 'tag': 'test_tag_1'}
        point = snowfloat.geometry.Point(coordinates=[4, 5, 6])
        self.feature.update(geometry=point, fields=fields)
        geojson = {'type': 'Feature',
             'geometry': {'type': 'Point',
                          'coordinates': [4, 5, 6]},
             'properties': {
                  'field_tag': 'test_tag_1',
                  'field_ts': 2,
              }}
        put_mock.assert_called_with(
            '%s/geo/1/layers/test_layer_1/'\
                'features/test_feature_1' % (tests.helper.URL_PREFIX),
            headers=tests.helper.get_request_headers(),
            data=json.dumps(geojson),
            params={},
            timeout=10,
            verify=False)
        self.assertListEqual(self.feature.geometry.coordinates, [4, 5, 6])
        self.assertDictEqual(self.feature.fields,
            {'ts': 2, 'tag': 'test_tag_1'})

    @patch.object(requests, 'delete')
    def test_delete(self, delete_mock):
        """Delete feature."""
        delete_mock.__name__ = 'delete'
        self.delete_feature_helper(delete_mock,
            self.feature.delete)


