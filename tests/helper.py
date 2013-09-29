"""Helpers for unit tests."""
import email.utils
import json
import unittest

from mock import Mock, call

import snowfloat.client
import snowfloat.errors
import snowfloat.geometry
import snowfloat.settings
import snowfloat.task

URL_PREFIX = 'https://%s' % (snowfloat.settings.HOST,)

class Tests(unittest.TestCase):
    """Parent class for unit tests."""
    
    # pylint: disable=C0103
    def setUp(self):
        snowfloat.settings.HOST = 'api.snowfloat.com:443'
        snowfloat.settings.HTTP_RETRY_INTERVAL = 0.1
        snowfloat.settings.API_KEY_ID = 'IY3487E2J6ZHFOW5A7P5'
        self.client = snowfloat.client.Client()

        self.features = []
        point = snowfloat.geometry.Point(coordinates=[1, 2, 3])
        fields = {'ts': 4, 'tag': 'test_tag_1'}
        feature = snowfloat.feature.Feature(point, fields=fields)
        self.features.append(feature)
        geometry = snowfloat.geometry.Polygon(
                  coordinates=[[[11, 12, 13],
                                [14, 15, 16],
                                [17, 18, 19],
                                [11, 12, 13]]])
        fields = {'ts': 11, 'tag': 'test_tag_2'}
        feature = snowfloat.feature.Feature(geometry, fields=fields)
        self.features.append(feature)
        geometry = snowfloat.geometry.MultiPolygon(
                  coordinates=[[[[11, 12, 13],
                                 [14, 15, 16],
                                 [17, 18, 19],
                                 [11, 12, 13]]],
                               [[[21, 22, 23],
                                 [24, 25, 26],
                                 [27, 28, 29],
                                 [21, 22, 23]]]])
        fields = {'ts': 21, 'tag': 'test_tag_3'}
        feature = snowfloat.feature.Feature(geometry, fields=fields)
        self.features.append(feature)
        geometry = snowfloat.geometry.LineString(
                  coordinates=[[11, 12, 13],
                               [14, 15, 16]])
        fields = {'ts': 31, 'tag': 'test_tag_4'}
        feature = snowfloat.feature.Feature(geometry, fields=fields)
        self.features.append(feature)
        multipoint = snowfloat.geometry.MultiPoint(
                  coordinates=[[11, 12, 13],
                               [14, 15, 16]])
        fields = {'ts': 41, 'tag': 'test_tag_5'}
        feature = snowfloat.feature.Feature(multipoint, fields=fields)
        self.features.append(feature)
        geometry = snowfloat.geometry.MultiLineString(
                  coordinates=[[[11, 12, 13],
                                [14, 15, 16],
                                [17, 18, 19],
                                [11, 12, 13]],
                               [[21, 22, 23],
                                [24, 25, 26],
                                [27, 28, 29],
                                [21, 22, 23]]])
        fields = {'ts': 51, 'tag': 'test_tag_6'}
        feature = snowfloat.feature.Feature(geometry, fields=fields)
        self.features.append(feature)
        geometry = snowfloat.geometry.GeometryCollection([point,
            multipoint])
        fields = {'ts': 61, 'tag': 'test_tag_7'}
        feature = snowfloat.feature.Feature(geometry, fields=fields)
        self.features.append(feature)
    
        email.utils.formatdate = Mock()
        email.utils.formatdate.return_value = 'Sat, 08 Jun 2013 22:12:05 GMT'
        self.get_sha = snowfloat.request._get_sha
        snowfloat.request._get_sha = Mock()
        snowfloat.request._get_sha.return_value = \
            'n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg='
        self.get_hmac_sha = snowfloat.request._get_hmac_sha
        snowfloat.request._get_hmac_sha = Mock()
        snowfloat.request._get_hmac_sha.return_value = \
            'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='

    # pylint: disable=C0103
    def tearDown(self):
        snowfloat.request._get_sha = self.get_sha
        snowfloat.request._get_hmac_sha = self.get_hmac_sha

    # pylint: disable=R0915
    def get_features_helper(self, method_mock, method, *args, **kwargs):
        """Helper for getting layer features."""
        r1 = {
                'next_page_uri':
                    '/geo/1/layers/test_layer_1/features?page=1'\
                    '&page_size=2',
                'total': 3,
                'geo': {
                      'type': 'FeatureCollection',
                      'features': [
                        {'type': 'Feature',
                         'id': 'test_point_1',
                         'geometry': {'type': 'Point',
                                      'coordinates': [1, 2, 3]},
                         'properties': {
                            'uri': '/geo/1/layers/test_layer_1/'\
                                'features/test_point_1',
                            'field_ts': 4,
                            'field_tag': 'test_tag_1',
                            'date_created': 5,
                            'date_modified': 6,
                            'spatial': {'type': 'Point',
                                        'coordinates': [4, 5, 6]}}
                        },
                        {'type': 'Feature',
                         'id': 'test_polygon_1',
                         'geometry': {'type': 'Polygon',
                                      'coordinates': [[[11, 12, 13],
                                                       [14, 15, 16],
                                                       [17, 18, 19],
                                                       [11, 12, 13],
                                                     ]]},
                         'properties': {
                            'uri': '/geo/1/layers/test_layer_1/'\
                                'features/test_polygon_1',
                            'field_ts': 11,
                            'field_tag': 'test_tag_2',
                            'date_created': 12,
                            'date_modified': 13,
                            'spatial': {'type': 'Point',
                                        'coordinates': [7, 8, 9]}}
                        },
                        {'type': 'Feature',
                         'id': 'test_multipolygon_1',
                         'geometry': {'type': 'MultiPolygon',
                                      'coordinates': [[[[11, 12, 13],
                                                        [14, 15, 16],
                                                        [17, 18, 19],
                                                        [11, 12, 13]]],
                                                      [[[21, 22, 23],
                                                        [24, 25, 26],
                                                        [27, 28, 29],
                                                        [21, 22, 23]]]]},
                         'properties': {
                            'uri': '/geo/1/layers/test_layer_1/'\
                                'features/test_multipolygon_1',
                            'field_ts': 21,
                            'field_tag': 'test_tag_3',
                            'date_created': 22,
                            'date_modified': 23,
                            'spatial': {'type': 'Point',
                                        'coordinates': [10, 11, 12]}}

                        },
                        {'type': 'Feature',
                         'id': 'test_linestring_1',
                         'geometry': {'type': 'LineString',
                                      'coordinates': [[11, 12, 13],
                                                      [14, 15, 16]
                                                     ]},
                         'properties': {
                            'uri': '/geo/1/layers/test_layer_1/'\
                                'features/test_linestring_1',
                            'field_ts': 31,
                            'field_tag': 'test_tag_4',
                            'date_created': 32,
                            'date_modified': 33,
                            'spatial': {'type': 'Point',
                                        'coordinates': [13, 14, 15]}}
                        },
                        {'type': 'Feature',
                         'id': 'test_multipoint_1',
                         'geometry': {'type': 'MultiPoint',
                                      'coordinates': [[11, 12, 13],
                                                      [14, 15, 16]
                                                     ]},
                         'properties': {
                            'uri': '/geo/1/layers/test_layer_1/'\
                                'features/test_multipoint_1',
                            'field_ts': 41,
                            'field_tag': 'test_tag_5',
                            'date_created': 42,
                            'date_modified': 43,
                            'spatial': {'type': 'Point',
                                        'coordinates': [16, 17, 18]}}
                        },
                        {'type': 'Feature',
                         'id': 'test_multilinestring_1',
                         'geometry': {'type': 'MultiLineString',
                                      'coordinates': [[[11, 12, 13],
                                                       [14, 15, 16],
                                                       [17, 18, 19],
                                                       [11, 12, 13]],
                                                      [[21, 22, 23],
                                                       [24, 25, 26],
                                                       [27, 28, 29],
                                                       [21, 22, 23]]]},
                         'properties': {
                            'uri': '/geo/1/layers/test_layer_1/'\
                                'features/test_multilinestring_1',
                            'field_ts': 51,
                            'field_tag': 'test_tag_6',
                            'date_created': 52,
                            'date_modified': 53,
                            'spatial': {'type': 'Point',
                                        'coordinates': [19, 20, 21]}}

                        },
                        {'type': 'Feature',
                         'id': 'test_geometrycollection_1',
                         'geometry': {
                            'type': 'GeometryCollection',
                            'geometries': [
                                      {'type': 'Point',
                                       'coordinates': [1, 2, 3],
                                                      },
                                      {'type': 'MultiPoint',
                                       'coordinates': [[11, 12, 13],
                                                       [14, 15, 16]
                                                      ]},
                                      ]},
                         'properties': {
                            'uri': '/geo/1/layers/test_layer_1/'\
                                'features/test_geometrycollection_1',
                            'field_ts': 61,
                            'field_tag': 'test_tag_7',
                            'date_created': 62,
                            'date_modified': 63,
                            'spatial': {'type': 'Point',
                                        'coordinates': [22, 23, 24]}}
                        },
                        {'type': 'Feature',
                         'id': 'test_geometry_1',
                         'geometry': None,
                         'properties': {
                            'uri': '/geo/1/layers/test_layer_1/'\
                                'features/test_geometry_1',
                            'field_ts': 71,
                            'field_tag': 'test_tag_8',
                            'date_created': 72,
                            'date_modified': 73,
                            'spatial': None}
                        },
                        ]}}
        r2 = {
              'next_page_uri': None,
              'total': 0,
              'geo': {'features': []}}
        m1 = Mock()
        m1.status_code = 200
        m1.json.return_value = r1
        m2 = Mock()
        m2.status_code = 200
        m2.json.return_value = r2
        method_mock.side_effect = [m1, m2]
        features = method(*args, **kwargs)
        self.assertEqual(len(features), 8)

        feature = features[0]
        self.assertListEqual(feature.geometry.coordinates, [1, 2, 3])
        self.assertEqual(feature.fields['ts'], 4)
        self.assertEqual(feature.fields['tag'], 'test_tag_1')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_point_1')
        self.assertEqual(feature.uuid, 'test_point_1')
        self.assertEqual(feature.date_created, 5)
        self.assertEqual(feature.date_modified, 6)
        self.assertEqual(feature.spatial.geometry_type, 'Point')
        self.assertListEqual(feature.spatial.coordinates, [4, 5, 6])
        self.assertEqual(str(feature),
            'Feature: uuid=test_point_1, '\
            'uri=/geo/1/layers/test_layer_1/features/test_point_1, '\
            'date_created=5, date_modified=6, '\
            'geometry=Point: coordinates=[1, 2, 3], '\
            'fields={\'tag\': \'test_tag_1\', \'ts\': 4}, '\
            'layer_uuid=test_layer_1, spatial=Point: coordinates=[4, 5, 6]')
        rpr = repr(feature).replace('Feature', 'snowfloat.feature.Feature')
        rpr = rpr.replace('Point', 'snowfloat.geometry.Point')
        feature = eval(rpr)
        self.assertTrue(isinstance(feature, snowfloat.feature.Feature))
        self.assertEqual(feature.uuid, 'test_point_1')
        self.assertEqual(feature.date_created, 5)
        self.assertEqual(feature.date_modified, 6)
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_point_1')
        self.assertDictEqual(feature.fields,
            {'tag': 'test_tag_1', 'ts': 4})
        self.assertEqual(feature.layer_uuid, 'test_layer_1')
        geometry = feature.geometry
        self.assertEqual(geometry.geometry_type, 'Point')
        self.assertListEqual(geometry.coordinates, [1, 2, 3])
        
        feature = features[1]
        self.assertListEqual(feature.geometry.coordinates, [[[11, 12, 13],
                                                             [14, 15, 16],
                                                             [17, 18, 19],
                                                             [11, 12, 13]]])
        self.assertEqual(feature.fields['ts'], 11)
        self.assertEqual(feature.fields['tag'], 'test_tag_2')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_polygon_1')
        self.assertEqual(feature.uuid, 'test_polygon_1')
        self.assertEqual(feature.date_created, 12)
        self.assertEqual(feature.date_modified, 13)
        self.assertEqual(feature.spatial.geometry_type, 'Point')
        self.assertListEqual(feature.spatial.coordinates, [7, 8, 9])
        
        feature = features[2]
        self.assertListEqual(feature.geometry.coordinates, [[[[11, 12, 13],
                                                              [14, 15, 16],
                                                              [17, 18, 19],
                                                              [11, 12, 13]]],
                                                            [[[21, 22, 23],
                                                              [24, 25, 26],
                                                              [27, 28, 29],
                                                              [21, 22, 23]]]])
        self.assertEqual(feature.fields['ts'], 21)
        self.assertEqual(feature.fields['tag'], 'test_tag_3')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_multipolygon_1')
        self.assertEqual(feature.uuid, 'test_multipolygon_1')
        self.assertEqual(feature.date_created, 22)
        self.assertEqual(feature.date_modified, 23)
        self.assertEqual(feature.spatial.geometry_type, 'Point')
        self.assertListEqual(feature.spatial.coordinates, [10, 11, 12])
        
        feature = features[3]
        self.assertListEqual(feature.geometry.coordinates, [[11, 12, 13],
                                                            [14, 15, 16]])
        self.assertEqual(feature.fields['ts'], 31)
        self.assertEqual(feature.fields['tag'], 'test_tag_4')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_linestring_1')
        self.assertEqual(feature.uuid, 'test_linestring_1')
        self.assertEqual(feature.date_created, 32)
        self.assertEqual(feature.date_modified, 33)
        self.assertEqual(feature.spatial.geometry_type, 'Point')
        self.assertListEqual(feature.spatial.coordinates, [13, 14, 15])
        
        feature = features[4]
        self.assertListEqual(feature.geometry.coordinates, [[11, 12, 13],
                                                            [14, 15, 16]])
        self.assertEqual(feature.fields['ts'], 41)
        self.assertEqual(feature.fields['tag'], 'test_tag_5')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_multipoint_1')
        self.assertEqual(feature.uuid, 'test_multipoint_1')
        self.assertEqual(feature.date_created, 42)
        self.assertEqual(feature.date_modified, 43)
        self.assertEqual(feature.spatial.geometry_type, 'Point')
        self.assertListEqual(feature.spatial.coordinates, [16, 17, 18])
        
        feature = features[5]
        self.assertListEqual(feature.geometry.coordinates, [[[11, 12, 13],
                                                             [14, 15, 16],
                                                             [17, 18, 19],
                                                             [11, 12, 13]],
                                                            [[21, 22, 23],
                                                             [24, 25, 26],
                                                             [27, 28, 29],
                                                             [21, 22, 23]]])
        self.assertEqual(feature.fields['ts'], 51)
        self.assertEqual(feature.fields['tag'], 'test_tag_6')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_multilinestring_1')
        self.assertEqual(feature.uuid, 'test_multilinestring_1')
        self.assertEqual(feature.date_created, 52)
        self.assertEqual(feature.date_modified, 53)
        self.assertEqual(feature.spatial.geometry_type, 'Point')
        self.assertListEqual(feature.spatial.coordinates, [19, 20, 21])

        feature = features[6]
        geometry = feature.geometry.geometries[0]
        self.assertListEqual(geometry.coordinates, [1, 2, 3])
        geometry = feature.geometry.geometries[1]
        self.assertListEqual(geometry.coordinates, [[11, 12, 13],
                                                    [14, 15, 16]])
        self.assertEqual(feature.fields['ts'], 61)
        self.assertEqual(feature.fields['tag'], 'test_tag_7')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_geometrycollection_1')
        self.assertEqual(feature.uuid, 'test_geometrycollection_1')
        self.assertEqual(feature.date_created, 62)
        self.assertEqual(feature.date_modified, 63)
        self.assertEqual(feature.spatial.geometry_type, 'Point')
        self.assertListEqual(feature.spatial.coordinates, [22, 23, 24])

        feature = features[7]
        self.assertIsNone(feature.geometry)
        self.assertEqual(feature.fields['ts'], 71)
        self.assertEqual(feature.fields['tag'], 'test_tag_8')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_geometry_1')
        self.assertEqual(feature.uuid, 'test_geometry_1')
        self.assertEqual(feature.date_created, 72)
        self.assertEqual(feature.date_modified, 73)
        self.assertIsNone(feature.spatial)

        distance_lookup = {'type': 'Point',
                           'coordinates': [1, 2, 3],
                           'properties': {'distance': 4}}
        spatial_geometry = {'type': 'Point',
                            'coordinates': [4, 5, 6]}
        self.assertEqual(method_mock.call_args_list,
            [call('%s/geo/1/layers/test_layer_1/features' % 
                (URL_PREFIX,),
                  headers=get_request_no_body_headers(),
                  params={'field_ts__gte': 1,
                          'field_ts__lte': 10,
                          'date_created__lte': '2002-12-25 00:00:00-00:00',
                          'geometry__distance_lte':
                            json.dumps(distance_lookup),
                          'spatial_operation': 'intersection',
                          'spatial_geometry':
                            json.dumps(spatial_geometry),
                          'spatial_flag': True,
                          'order_by': '-field_ts,date_created',
                          'slice_start': 1,
                          'slice_end': 20},
                  data={},
                  timeout=10,
                  verify=False),
             call('%s/geo/1/layers/test_layer_1/features?page=1'\
                    '&page_size=2'
                  % (URL_PREFIX,),
                  headers=get_request_no_body_headers(),
                  params={},
                  data={},
                  timeout=10,
                  verify=False)])

    # pylint: disable=R0915
    def add_features_helper(self, method_mock, method, *args, **kwargs):
        """Helper for adding layer features."""
        r = {
             'type': 'FeatureCollection',
             'features': [
               {'type': 'Feature',
                'id': 'test_point_1',
                'geometry': {'type': 'Point', 'coordinates': [1, 2, 3]},
                'properties': {
                   'uri': '/geo/1/layers/test_layer_1/'\
                          'features/test_point_1',
                   'field_tag': 'test_tag_1',
                   'field_ts': 4,
                   'date_created': 5,
                   'date_modified': 6}
               },
               {'type': 'Feature',
                'id': 'test_polygon_1',
                'geometry': {'type': 'Polygon',
                             'coordinates': [[[11, 12, 13],
                                              [14, 15, 16],
                                              [17, 18, 19],
                                              [11, 12, 13],
                                            ]]},
                'properties': {
                   'uri': '/geo/1/layers/test_layer_1/'\
                          'features/test_polygon_1',
                   'field_tag': 'test_tag_2',
                   'field_ts': 11,
                   'date_created': 12,
                   'date_modified': 13}
               },
                {'type': 'Feature',
                 'id': 'test_multipolygon_1',
                 'geometry': {'type': 'MultiPolygon',
                              'coordinates': [[[[11, 12, 13],
                                                [14, 15, 16],
                                                [17, 18, 19],
                                                [11, 12, 13]]],
                                              [[[21, 22, 23],
                                                [24, 25, 26],
                                                [27, 28, 29],
                                                [21, 22, 23]]]]},
                 'properties': {
                    'uri': '/geo/1/layers/test_layer_1/'\
                        'features/test_multipolygon_1',
                    'field_tag': 'test_tag_3',
                    'field_ts': 21,
                    'date_created': 22,
                    'date_modified': 23}
                },
                {'type': 'Feature',
                 'id': 'test_linestring_1',
                 'geometry': {'type': 'LineString',
                              'coordinates': [[11, 12, 13],
                                              [14, 15, 16]
                                             ]},
                 'properties': {
                    'uri': '/geo/1/layers/test_layer_1/'\
                        'features/test_linestring_1',
                    'field_tag': 'test_tag_4',
                    'field_ts': 31,
                    'date_created': 32,
                    'date_modified': 33},
                },
                {'type': 'Feature',
                 'id': 'test_multipoint_1',
                 'geometry': {'type': 'MultiPoint',
                              'coordinates': [[11, 12, 13],
                                              [14, 15, 16]
                                             ]},
                 'properties': {
                    'uri': '/geo/1/layers/test_layer_1/'\
                        'features/test_multipoint_1',
                    'field_tag': 'test_tag_5',
                    'field_ts': 41,
                    'date_created': 42,
                    'date_modified': 43},
                },
                {'type': 'Feature',
                 'id': 'test_multilinestring_1',
                 'geometry': {'type': 'MultiLineString',
                              'coordinates': [[[11, 12, 13],
                                               [14, 15, 16],
                                               [17, 18, 19],
                                               [11, 12, 13]],
                                              [[21, 22, 23],
                                               [24, 25, 26],
                                               [27, 28, 29],
                                               [21, 22, 23]]]},
                 'properties': {
                    'uri': '/geo/1/layers/test_layer_1/'\
                        'features/test_multilinestring_1',
                    'field_tag': 'test_tag_6',
                    'field_ts': 51,
                    'date_created': 52,
                    'date_modified': 53}
                },
                {'type': 'Feature',
                 'id': 'test_geometrycollection_1',
                 'geometry': {
                    'type': 'GeometryCollection',
                    'geometries': [
                              {'type': 'Point',
                               'coordinates': [1, 2, 3],
                                              },
                              {'type': 'MultiPoint',
                               'coordinates': [[11, 12, 13],
                                               [14, 15, 16]
                                              ]},
                              ]},
                 'properties': {
                    'uri': '/geo/1/layers/test_layer_1/'\
                        'features/test_geometrycollection_1',
                    'field_tag': 'test_tag_7',
                    'field_ts': 61,
                    'date_created': 62,
                    'date_modified': 63}
                },
                ]}
        m = Mock()
        m.status_code = 200
        m.json.return_value = r
        method_mock.return_value = m
        features = method(*args, **kwargs)
        
        feature = features[0]
        self.assertListEqual(feature.geometry.coordinates, [1, 2, 3])
        self.assertEqual(feature.fields['ts'], 4)
        self.assertEqual(feature.fields['tag'], 'test_tag_1')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_point_1')
        self.assertEqual(feature.layer_uuid, 'test_layer_1')
        self.assertEqual(feature.uuid, 'test_point_1')
        self.assertEqual(feature.date_created, 5)
        self.assertEqual(feature.date_modified, 6)
        
        feature = features[1]
        self.assertListEqual(feature.geometry.coordinates, [[[11, 12, 13],
                                                             [14, 15, 16],
                                                             [17, 18, 19],
                                                             [11, 12, 13]]])
        self.assertEqual(feature.fields['ts'], 11)
        self.assertEqual(feature.fields['tag'], 'test_tag_2')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_polygon_1')
        self.assertEqual(feature.layer_uuid, 'test_layer_1')
        self.assertEqual(feature.uuid, 'test_polygon_1')
        self.assertEqual(feature.date_created, 12)
        self.assertEqual(feature.date_modified, 13)
        feature = features[2]
        self.assertListEqual(feature.geometry.coordinates, [[[[11, 12, 13],
                                                              [14, 15, 16],
                                                              [17, 18, 19],
                                                              [11, 12, 13]]],
                                                            [[[21, 22, 23],
                                                              [24, 25, 26],
                                                              [27, 28, 29],
                                                              [21, 22, 23]]]])
        self.assertEqual(feature.fields['ts'], 21)
        self.assertEqual(feature.fields['tag'], 'test_tag_3')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_multipolygon_1')
        self.assertEqual(feature.layer_uuid, 'test_layer_1')
        self.assertEqual(feature.uuid, 'test_multipolygon_1')
        self.assertEqual(feature.date_created, 22)
        self.assertEqual(feature.date_modified, 23)
        
        feature = features[3]
        self.assertListEqual(feature.geometry.coordinates, [[11, 12, 13],
                                                            [14, 15, 16]])
        self.assertEqual(feature.fields['ts'], 31)
        self.assertEqual(feature.fields['tag'], 'test_tag_4')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_linestring_1')
        self.assertEqual(feature.layer_uuid, 'test_layer_1')
        self.assertEqual(feature.uuid, 'test_linestring_1')
        self.assertEqual(feature.date_created, 32)
        self.assertEqual(feature.date_modified, 33)
        
        feature = features[4]
        self.assertListEqual(feature.geometry.coordinates, [[11, 12, 13],
                                                            [14, 15, 16]])
        self.assertEqual(feature.fields['ts'], 41)
        self.assertEqual(feature.fields['tag'], 'test_tag_5')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_multipoint_1')
        self.assertEqual(feature.layer_uuid, 'test_layer_1')
        self.assertEqual(feature.uuid, 'test_multipoint_1')
        self.assertEqual(feature.date_created, 42)
        self.assertEqual(feature.date_modified, 43)
        
        feature = features[5]
        self.assertListEqual(feature.geometry.coordinates, [[[11, 12, 13],
                                                             [14, 15, 16],
                                                             [17, 18, 19],
                                                             [11, 12, 13]],
                                                            [[21, 22, 23],
                                                             [24, 25, 26],
                                                             [27, 28, 29],
                                                             [21, 22, 23]]])
        self.assertEqual(feature.fields['ts'], 51)
        self.assertEqual(feature.fields['tag'], 'test_tag_6')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_multilinestring_1')
        self.assertEqual(feature.layer_uuid, 'test_layer_1')
        self.assertEqual(feature.uuid, 'test_multilinestring_1')
        self.assertEqual(feature.date_created, 52)
        self.assertEqual(feature.date_modified, 53)
       
        feature = features[6]
        geometry = feature.geometry.geometries[0]
        self.assertListEqual(geometry.coordinates, [1, 2, 3])
        geometry = feature.geometry.geometries[1]
        self.assertListEqual(geometry.coordinates, [[11, 12, 13],
                                                    [14, 15, 16]])
        self.assertEqual(feature.fields['ts'], 61)
        self.assertEqual(feature.fields['tag'], 'test_tag_7')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_geometrycollection_1')
        self.assertEqual(feature.uuid, 'test_geometrycollection_1')
        self.assertEqual(feature.date_created, 62)
        self.assertEqual(feature.date_modified, 63)
 
        for i in range(7):
            del r['features'][i]['id']
            del r['features'][i]['properties']['uri']
            del r['features'][i]['properties']['date_created']
            del r['features'][i]['properties']['date_modified']
        self.assertEqual(method_mock.call_args_list,
            [call('%s/geo/1/layers/test_layer_1/features'
                    % (URL_PREFIX,),
                  headers=get_request_body_headers(),
                  data=json.dumps(r),
                  params={},
                  timeout=10,
                  verify=False)])

    # pylint: disable=R0201
    def delete_features_helper(self, method_mock, method, *args, **kwargs):
        """Helper for deleting layer features."""
        m = Mock()
        m.status_code = 200
        m.json.return_value = {'num_features': 2, 'num_points': 1}
        method_mock.return_value = m
        method(*args, **kwargs)
        method_mock.assert_called_with(
            '%s/geo/1/layers/test_layer_1/features' % (URL_PREFIX),
            headers=get_request_no_body_headers(),
            params={'field_ts__gte': 1, 'field_ts__lte': 10,
                    'date_created__lte': '2002-12-25 00:00:00-00:00'},
            data={},
            timeout=10,
            verify=False)

    # pylint: disable=R0201
    def delete_feature_helper(self, method_mock, method, *args, **kwargs):
        """Helper for deleting a layer feature."""
        m = Mock()
        m.status_code = 200
        m.json.return_value = {'num_points': 1}
        method_mock.return_value = m
        method(*args, **kwargs)
        method_mock.assert_called_with(
            '%s/geo/1/layers/test_layer_1/'\
                'features/test_feature_1' % (URL_PREFIX),
            headers=get_request_no_body_headers(),
            params={},
            data={},
            timeout=10,
            verify=False)

    def get_results_helper(self, method_mock, method, *args, **kwargs):
        """Helper for getting task results."""
        r1 = {
              'next_page_uri':
                    '/geo/1/tasks/test_task_1/results?page=1&page_size=2',
              'total': 2,
              'results': [
                {
                  'uuid': 'test_result_1',
                  'uri': '/geo/1/tasks/test_task_1/results/test_result_1',
                  'tag': 'test_tag_1',
                  'date_created': 1,
                  'date_modified': 2,
                },
                {
                  'uuid': 'test_result_2',
                  'uri': '/geo/1/tasks/test_task_1/results/test_result_2',
                  'tag': 'test_tag_2',
                  'date_created': 3,
                  'date_modified': 4,
                }]}
        r2 = {
              'next_page_uri': None,
              'total': 0,
              'results': [],}
        m1 = Mock()
        m1.status_code = 200
        m1.json.return_value = r1
        m2 = Mock()
        m2.status_code = 200
        m2.json.return_value = r2
        method_mock.side_effect = [m1, m2]
        results = [e for e in method(*args, **kwargs)]
        
        result = results[0]
        self.assertEqual(result.uuid, 'test_result_1')
        self.assertEqual(result.uri,
            '/geo/1/tasks/test_task_1/results/test_result_1')
        self.assertEqual(result.tag, 'test_tag_1')
        self.assertEqual(result.date_created, 1)
        self.assertEqual(result.date_modified, 2)
        self.assertEqual(str(result),
            'Result: uuid=test_result_1, ' \
            'uri=/geo/1/tasks/test_task_1/results/test_result_1, ' \
            'date_created=1, date_modified=2, tag=test_tag_1')
        result = eval('snowfloat.result.' + repr(result))
        self.assertTrue(isinstance(result, snowfloat.result.Result))
        self.assertEqual(result.uuid, 'test_result_1')
        self.assertEqual(result.uri,
            '/geo/1/tasks/test_task_1/results/test_result_1')
        self.assertEqual(result.tag, 'test_tag_1')
        self.assertEqual(result.date_created, 1)
        self.assertEqual(result.date_modified, 2)
        
        result = results[1]
        self.assertEqual(result.uuid, 'test_result_2')
        self.assertEqual(result.uri,
            '/geo/1/tasks/test_task_1/results/test_result_2')
        self.assertEqual(result.tag, 'test_tag_2')
        self.assertEqual(result.date_created, 3)
        self.assertEqual(result.date_modified, 4)
        self.method_mock_assert_call_args_list(method_mock,
            '/geo/1/tasks/test_task_1/results',
            '/geo/1/tasks/test_task_1/results?page=1&page_size=2')

    def method_mock_assert_call_args_list(self, mock, uri_1, uri_2,
            params=None):
        """Requests method mock assert call args list helper."""
        if not params:
            params = {}
        self.assertEqual(mock.call_args_list,
            [call('%s%s' % (URL_PREFIX, uri_1),
                  headers=get_request_no_body_headers(),
                  data={},
                  params=params,
                  timeout=10,
                  verify=False),
             call('%s%s' % (
                      URL_PREFIX, uri_2),
                  headers=get_request_no_body_headers(),
                  data={},
                  params={},
                  timeout=10,
                  verify=False)])

    def get_features_test(self, mock, *args):
        """Client or layer get features test."""
        mock.__name__ = 'get'
        point = snowfloat.geometry.Point(coordinates=[1, 2, 3])
        point2 = snowfloat.geometry.Point(coordinates=[4, 5, 6])
        self.get_features_helper(mock,
            *args,
            field_ts_gte=1,
            field_ts_lte=10,
            date_created_lte='2002-12-25 00:00:00-00:00',
            query='distance_lte',
            geometry=point,
            distance=4,
            spatial_operation='intersection',
            spatial_geometry=point2,
            spatial_flag=True,
            order_by=('-field_ts', 'date_created'),
            query_slice=(1, 20))
 
def method_mock_assert_called_with(mock, uri):
    """Requests method mock assert called with helper."""
    mock.assert_called_with(
        '%s%s' % (URL_PREFIX, uri),
        headers=get_request_no_body_headers(),
        data={},
        params={},
        timeout=10,
        verify=False)

def set_method_mock(method_mock, name, status_code, return_value):
    """Set method mock return value with mock."""
    method_mock.__name__ = name
    mock = Mock()
    mock.status_code = status_code
    mock.json.return_value = return_value
    method_mock.return_value = mock
                
def get_request_no_body_headers():
    """Get headers for a request without a body."""
    return {'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
            'Authorization':
                'GEO IY3487E2J6ZHFOW5A7P5:'\
                'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='}

def get_request_body_headers(content_type='application/json'):
    """Get headers for a request with a body."""
    return {'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
            'Content-Type': content_type,
            'Authorization':
               'GEO IY3487E2J6ZHFOW5A7P5:'\
               'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I=',
            'Content-Sha':
               'n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg='}


