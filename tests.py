import email.utils
import json
import os
import sys
import tempfile
import unittest

from mock import Mock, patch, call
import requests
import requests.exceptions

import snowfloat.client
import snowfloat.errors
import snowfloat.geometry
import snowfloat.settings
import snowfloat.task

url_prefix = 'https://%s' % (snowfloat.settings.HOST,)

class Tests(unittest.TestCase):
   
    def setUp(self):
        snowfloat.settings.HOST = 'api.snowfloat.com:443'
        snowfloat.settings.HTTP_RETRY_INTERVAL = 0.1
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

    def tearDown(self):
        snowfloat.request._get_sha = self.get_sha
        snowfloat.request._get_hmac_sha = self.get_hmac_sha

    def get_features_helper(self, method_mock, method, *args, **kwargs):
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
            'Feature(uuid=test_point_1, '\
            'uri=/geo/1/layers/test_layer_1/features/test_point_1, '\
            'date_created=5, date_modified=6, '\
            'geometry=Point(coordinates=[1, 2, 3]), '\
            'fields={\'tag\': \'test_tag_1\', \'ts\': 4}, '\
            'layer_uuid=test_layer_1, spatial=Point(coordinates=[4, 5, 6])')
        
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
                (url_prefix,),
                  headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                           'Authorization':
                               'GEO IY3487E2J6ZHFOW5A7P5:'\
                               'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='},
                  params={'field_ts__gte': 1,
                          'field_ts__lte': 10,
                          'date_created__lte': '2002-12-25 00:00:00-00:00',
                          'geometry__distance_lte':
                            json.dumps(distance_lookup),
                          'spatial_operation': 'intersection',
                          'spatial_geometry':
                            json.dumps(spatial_geometry),
                          'spatial_flag': True,
                          'order_by': '-field_ts,date_created'},
                  data={},
                  timeout=10,
                  verify=False),
             call('%s/geo/1/layers/test_layer_1/features?page=1'\
                    '&page_size=2'
                  % (url_prefix,),
                  headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                           'Authorization':
                               'GEO IY3487E2J6ZHFOW5A7P5:'\
                               'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='},
                  params={},
                  data={},
                  timeout=10,
                  verify=False)])

    def add_features_helper(self, method_mock, method, *args, **kwargs):
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
                    % (url_prefix,),
                  headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                           'Content-Type': 'application/json',
                           'Content-Sha':
                               'n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg=',
                           'Authorization':
                               'GEO IY3487E2J6ZHFOW5A7P5:'\
                               'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='},
                  data=json.dumps(r),
                  params={},
                  timeout=10,
                  verify=False)])

    def delete_features_helper(self, method_mock, method, *args, **kwargs):
        m = Mock()
        m.status_code = 200
        m.json.return_value = {'num_features': 2, 'num_points': 1}
        method_mock.return_value = m
        method(*args, **kwargs)
        method_mock.assert_called_with(
            '%s/geo/1/layers/test_layer_1/features' % (url_prefix),
            headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                     'Authorization':
                         'GEO IY3487E2J6ZHFOW5A7P5:'\
                         'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='},
            params={'field_ts__gte': 1, 'field_ts__lte': 10,
                    'date_created__lte': '2002-12-25 00:00:00-00:00'},
            data={},
            timeout=10,
            verify=False)

    def delete_feature_helper(self, method_mock, method, *args, **kwargs):
        m = Mock()
        m.status_code = 200
        m.json.return_value = {'num_points': 1}
        method_mock.return_value = m
        method(*args, **kwargs)
        method_mock.assert_called_with(
            '%s/geo/1/layers/test_layer_1/'\
                'features/test_feature_1' % (url_prefix),
            headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                     'Authorization':
                         'GEO IY3487E2J6ZHFOW5A7P5:'\
                         'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='},
            params={},
            data={},
            timeout=10,
            verify=False)

    def get_results_helper(self, method_mock, method, *args, **kwargs):
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
        result = results[1]
        self.assertEqual(result.uuid, 'test_result_2')
        self.assertEqual(result.uri,
            '/geo/1/tasks/test_task_1/results/test_result_2')
        self.assertEqual(result.tag, 'test_tag_2')
        self.assertEqual(result.date_created, 3)
        self.assertEqual(result.date_modified, 4)
        self.assertEqual(method_mock.call_args_list,
            [call('%s/geo/1/tasks/test_task_1/results' % (url_prefix,),
                  headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                           'Authorization':
                               'GEO IY3487E2J6ZHFOW5A7P5:'\
                               'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='},
                  data={},
                  params={},
                  timeout=10,
                  verify=False),
             call('%s/geo/1/tasks/test_task_1/results?page=1&page_size=2'
                  % (url_prefix,),
                  headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                           'Authorization':
                               'GEO IY3487E2J6ZHFOW5A7P5:'\
                               'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='},
                  data={},
                  params={},
                  timeout=10,
                  verify=False)])


class ClientTests(Tests):
   
    @patch.object(requests, 'get')
    def test_get_layers(self, get_mock):
        get_mock.__name__ = 'get'
        r1 = {
                'next_page_uri': '/geo/1/layers?page=1&page_size=2',
                'total': 2,
                'layers': [{'name': 'test_tag_1',
                            'uri': '/geo/1/layers/test_layer_1',
                            'uuid': 'test_layer_1',
                            'date_created': 1,
                            'date_modified': 2,
                            'num_features': 10,
                            'num_points': 20,
                            'fields': [{'name': 'field_1', 'type': 'string',
                                        'size': 256},],
                            'srs': {'type': 'EPSG',
                                    'properties': {'code': 4326, 'dim': 3}},
                            'extent': [1, 2, 3, 4]
                           },
                           {'name': 'test_tag_2',
                            'uri': '/geo/1/layers/test_layer_2',
                            'uuid': 'test_layer_2',
                            'date_created': 3,
                            'date_modified': 4,
                            'num_features': 11,
                            'num_points': 21,
                            'fields': [{'name': 'field_2', 'type': 'string',
                                        'size': 256},],
                            'srs': {'type': 'EPSG',
                                    'properties': {'code': 4327, 'dim': 2}},
                            'extent': None
                           }],
            }
        r2 = {
                'next_page_uri': None,
                'total': 0,
                'layers': [],
            }
        m1 = Mock()
        m1.status_code = 200
        m1.json.return_value = r1
        m2 = Mock()
        m2.status_code = 200
        m2.json.return_value = r2
        get_mock.side_effect = [m1, m2]
        layers = self.client.get_layers(name_exact='test_name')
        
        self.assertEqual(layers[0].name, 'test_tag_1')
        self.assertEqual(layers[0].uri,
            '/geo/1/layers/test_layer_1')
        self.assertEqual(layers[0].uuid, 'test_layer_1')
        self.assertEqual(layers[0].date_created, 1)
        self.assertEqual(layers[0].date_modified, 2)
        self.assertEqual(layers[0].num_features, 10)
        self.assertEqual(layers[0].num_points, 20)
        self.assertListEqual(layers[0].fields,
            [{'name': 'field_1', 'type': 'string', 'size': 256},])
        self.assertDictEqual(layers[0].srs,
            {'type': 'EPSG', 'properties': {'code': 4326, 'dim': 3}})
        self.assertListEqual(layers[0].extent, [1, 2, 3, 4])
        self.assertEqual(str(layers[0]),
            'Layer(name=test_tag_1, uuid=test_layer_1, date_created=1, '\
            'date_modified=2, uri=/geo/1/layers/test_layer_1, '\
            'num_features=10, num_points=20, '\
            'fields=[{\'type\': \'string\', \'name\': \'field_1\', '\
            '\'size\': 256}], srs={\'type\': \'EPSG\', '\
            '\'properties\': {\'dim\': 3, \'code\': 4326}}, '\
            'extent=[1, 2, 3, 4])')

        self.assertEqual(layers[1].name, 'test_tag_2')
        self.assertEqual(layers[1].uri,
            '/geo/1/layers/test_layer_2')
        self.assertEqual(layers[1].uuid, 'test_layer_2')
        self.assertEqual(layers[1].date_created, 3)
        self.assertEqual(layers[1].date_modified, 4)
        self.assertEqual(layers[1].num_features, 11)
        self.assertEqual(layers[1].num_points, 21)
        self.assertListEqual(layers[1].fields,
            [{'name': 'field_2', 'type': 'string', 'size': 256},])
        self.assertDictEqual(layers[1].srs,
            {'type': 'EPSG', 'properties': {'code': 4327, 'dim': 2}})
        self.assertIsNone(layers[1].extent)
        self.assertEqual(get_mock.call_args_list,
            [call('%s/geo/1/layers' % (url_prefix,),
                  headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                           'Authorization':
                               'GEO IY3487E2J6ZHFOW5A7P5:'\
                               'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='},
                  data={},
                  params={'name__exact': 'test_name'},
                  timeout=10,
                  verify=False),
             call('%s/geo/1/layers?page=1&page_size=2' % (url_prefix,),
                  headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                           'Authorization':
                               'GEO IY3487E2J6ZHFOW5A7P5:'\
                               'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='},
                  data={},
                  params={},
                  timeout=10,
                  verify=False)])

    @patch.object(requests, 'get')
    def test_get_layers_status_code_413(self, get_mock):
        get_mock.__name__ = 'get'
        m = Mock()
        m.status_code = 413
        m.json.return_value = {'code': 1, 'message': 'test_message',
            'more': 'test_more'}
        get_mock.return_value = m
        self.assertRaises(snowfloat.errors.RequestError,
            self.client.get_layers)

    @patch.object(requests, 'get')
    def test_get_layers_get_error(self, get_mock):
        get_mock.__name__ = 'get'
        get_mock.side_effect = requests.exceptions.RequestException('timeout')
        self.assertRaises(snowfloat.errors.RequestError,
            self.client.get_layers)

    @patch.object(requests, 'post')
    def test_add_layers(self, post_mock):
        post_mock.__name__ = 'post'
        r = [{'name': 'test_tag_1',
              'uri': '/geo/1/layers/test_layer_1',
              'uuid': 'test_layer_1',
              'date_created': 1,
              'date_modified': 2,
              'num_features': 0,
              'num_points': 0,
              'fields': [{'name': 'field_1', 'type': 'string', 'size': 256},],
              'srs': {'type': 'EPSG', 'properties': {'code': 4326, 'dim': 3}},
              'extent': [1, 2, 3, 4]
             },
             {'name': 'test_tag_2',
              'uri': '/geo/1/layers/test_layer_2',
              'uuid': 'test_layer_2',
              'date_created': 3,
              'date_modified': 4,
              'num_features': 0,
              'num_points': 0,
              'fields': [{'name': 'field_2', 'type': 'string', 'size': 256},],
              'srs': {'type': 'EPSG', 'properties': {'code': 4327, 'dim': 2}},
              'extent': None
             }]
        m = Mock()
        m.status_code = 200
        m.json.return_value = r
        post_mock.return_value = m
        layers = [
            snowfloat.layer.Layer(name='test_tag_1',
                fields=[{'name': 'field_1', 'type': 'string', 'size': 256},],
                srs={'type': 'EPSG', 'properties': {'code': 4326, 'dim': 3}},
                extent=[1, 2, 3, 4]),
            snowfloat.layer.Layer(name='test_tag_2',
                fields=[{'name': 'field_2', 'type': 'string', 'size': 256},],
                srs={'type': 'EPSG', 'properties': {'code': 4327, 'dim': 2}}),
        ]
        layers = self.client.add_layers(layers)
        self.assertEqual(layers[0].name, 'test_tag_1')
        self.assertEqual(layers[0].uri,
            '/geo/1/layers/test_layer_1')
        self.assertEqual(layers[0].uuid, 'test_layer_1')
        self.assertEqual(layers[0].date_created, 1)
        self.assertEqual(layers[0].date_modified, 2)
        self.assertEqual(layers[0].num_features, 0)
        self.assertEqual(layers[0].num_points, 0)
        self.assertListEqual(layers[0].fields,
            [{'name': 'field_1', 'type': 'string', 'size': 256},])
        self.assertDictEqual(layers[0].srs,
            {'type': 'EPSG', 'properties': {'code': 4326, 'dim': 3}})
        self.assertListEqual(layers[0].extent, [1, 2, 3, 4])
        self.assertEqual(layers[1].name, 'test_tag_2')
        self.assertEqual(layers[1].uri,
            '/geo/1/layers/test_layer_2')
        self.assertEqual(layers[1].uuid, 'test_layer_2')
        self.assertEqual(layers[1].date_created, 3)
        self.assertEqual(layers[1].date_modified, 4)
        self.assertEqual(layers[1].num_features, 0)
        self.assertEqual(layers[1].num_points, 0)
        self.assertListEqual(layers[1].fields,
            [{'name': 'field_2', 'type': 'string', 'size': 256},])
        self.assertDictEqual(layers[1].srs,
            {'type': 'EPSG', 'properties': {'code': 4327, 'dim': 2}})
        self.assertIsNone(layers[1].extent)
        
        self.assertEqual(post_mock.call_args_list,
            [call('%s/geo/1/layers' % (url_prefix,),
                  headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                           'Content-Type': 'application/json',
                           'Authorization':
                               'GEO IY3487E2J6ZHFOW5A7P5:'\
                               'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I=',
                           'Content-Sha':
                               'n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg='},
                  data=json.dumps([
                    {'name': 'test_tag_1',
                     'fields': [{'name': 'field_1', 'type': 'string',
                                 'size': 256},],
                     'srs': {'type': 'EPSG',
                             'properties': {'code': 4326, 'dim': 3}},
                     'extent': [1, 2, 3, 4]},
                    {'name': 'test_tag_2',
                     'fields': [{'name': 'field_2', 'type': 'string',
                                 'size': 256},],
                     'srs': {'type': 'EPSG',
                             'properties': {'code': 4327, 'dim': 2}}},
                    ]),
                  params={},
                  timeout=10,
                  verify=False)])

    @patch.object(requests, 'delete')
    def test_delete_layers(self, delete_mock):
        delete_mock.__name__ = 'delete'
        m = Mock()
        m.status_code = 200
        m.json.return_value = {}
        delete_mock.return_value = m
        self.client.delete_layers()
        delete_mock.assert_called_with(
            '%s/geo/1/layers' % (url_prefix),
            headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                     'Authorization':
                         'GEO IY3487E2J6ZHFOW5A7P5:'\
                         'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='},
            data={},
            params={},
            timeout=10,
            verify=False)

    @patch.object(requests, 'get')
    def test_get_features(self, get_mock):
        get_mock.__name__ = 'get'
        point = snowfloat.geometry.Point(coordinates=[1, 2, 3])
        point2 = snowfloat.geometry.Point(coordinates=[4, 5, 6])
        self.get_features_helper(get_mock, self.client.get_features,
            'test_layer_1', field_ts_gte=1, field_ts_lte=10,
            date_created_lte='2002-12-25 00:00:00-00:00',
            query='distance_lte',
            geometry=point, distance=4, spatial_operation='intersection',
            spatial_geometry=point2, spatial_flag=True,
            order_by=('-field_ts', 'date_created'))

    @patch.object(requests, 'post')
    def test_add_features(self, post_mock):
        post_mock.__name__ = 'post'
        self.add_features_helper(post_mock, self.client.add_features,
            'test_layer_1', self.features)

    @patch.object(requests, 'delete')
    def test_delete_features(self, delete_mock):
        delete_mock.__name__ = 'delete'
        self.delete_features_helper(delete_mock,
            self.client.delete_features,
            'test_layer_1', field_ts_gte=1, field_ts_lte=10,
            date_created_lte='2002-12-25 00:00:00-00:00')

    @patch.object(requests, 'post')
    def test_add_tasks(self, post_mock):
        post_mock.__name__ = 'post'
        r = [{'operation': 'test_operation_1',
              'task_filter': {'filter_1': 'test_task_filter_1'},
              'spatial': {'spatial_1': 'test_task_spatial_1'},
              'uri': '/geo/1/tasks/test_task_1',
              'uuid': 'test_task_1',
              'state': 'started',
              'extras': {'extra': 'test_extra_1'},
              'reason': 'test_reason_1',
              'date_created': 1,
              'date_modified': 2
             },
             {'operation': 'test_operation_2',
              'task_filter': {'filter_2': 'test_task_filter_2'},
              'spatial': {'spatial_2': 'test_task_spatial_2'},
              'uri': '/geo/1/tasks/test_task_2',
              'uuid': 'test_task_2',
              'state': 'started',
              'extras': {'extra': 'test_extra_2'},
              'reason': 'test_reason_2',
              'date_created': 3,
              'date_modified': 4
             }]
        m = Mock()
        m.status_code = 200
        m.json.return_value = r
        post_mock.return_value = m
        tasks = [{'operation': 'test_operation_1'},
                 {'operation': 'test_operation_2'}]
        tasks = self.client._add_tasks(tasks)
        self.assertEqual(tasks[0].operation, 'test_operation_1')
        self.assertDictEqual(tasks[0].task_filter, 
            {'filter_1': 'test_task_filter_1'})
        self.assertDictEqual(tasks[0].spatial, 
            {'spatial_1': 'test_task_spatial_1'})
        self.assertEqual(tasks[0].uuid, 'test_task_1')
        self.assertEqual(tasks[0].state, 'started')
        self.assertDictEqual(tasks[0].extras, {'extra': 'test_extra_1'})
        self.assertEqual(tasks[0].reason, 'test_reason_1')
        self.assertEqual(tasks[0].uri, '/geo/1/tasks/test_task_1')
        self.assertEqual(tasks[0].date_created, 1)
        self.assertEqual(tasks[0].date_modified, 2)
        self.assertEqual(str(tasks[0]),
            'Task(uuid=test_task_1, uri=/geo/1/tasks/test_task_1, '\
            'date_created=1, date_modified=2, operation=test_operation_1, '\
            'task_filter={\'filter_1\': \'test_task_filter_1\'}, '\
            'spatial={\'spatial_1\': \'test_task_spatial_1\'} state=started, '\
            'extras={\'extra\': \'test_extra_1\'} reason=test_reason_1')
        self.assertEqual(tasks[1].operation, 'test_operation_2')
        self.assertDictEqual(tasks[1].task_filter, 
            {'filter_2': 'test_task_filter_2'})
        self.assertDictEqual(tasks[1].spatial, 
            {'spatial_2': 'test_task_spatial_2'})
        self.assertEqual(tasks[1].uuid, 'test_task_2')
        self.assertEqual(tasks[1].state, 'started')
        self.assertDictEqual(tasks[1].extras, {'extra': 'test_extra_2'})
        self.assertEqual(tasks[1].reason, 'test_reason_2')
        self.assertEqual(tasks[1].uri, '/geo/1/tasks/test_task_2')
        self.assertEqual(tasks[1].date_created, 3)
        self.assertEqual(tasks[1].date_modified, 4)

    @patch.object(requests, 'get')
    def test_get_task(self, get_mock):
        get_mock.__name__ = 'get'
        r = {'operation': 'test_operation_1',
             'task_filter': {'filter_1': 'test_task_filter_1'},
             'spatial': {'spatial_1': 'test_task_spatial_1'},
             'uri': '/geo/1/tasks/test_task_1',
             'uuid': 'test_task_1',
             'state': 'started',
             'extras': {'extra': 'test_extra_1'},
             'reason': 'test_reason_1',
             'date_created': 1,
             'date_modified': 2
            }
        m = Mock()
        m.status_code = 200
        m.json.return_value = r
        get_mock.return_value = m
        task = self.client._get_task('test_task_1')
        self.assertEqual(task.operation, 'test_operation_1')
        self.assertDictEqual(task.task_filter, 
            {'filter_1': 'test_task_filter_1'})
        self.assertDictEqual(task.spatial, 
            {'spatial_1': 'test_task_spatial_1'})
        self.assertEqual(task.uri, '/geo/1/tasks/test_task_1')
        self.assertEqual(task.uuid, 'test_task_1')
        self.assertEqual(task.state, 'started')
        self.assertDictEqual(task.extras, {'extra': 'test_extra_1'})
        self.assertEqual(task.reason, 'test_reason_1')
        self.assertEqual(task.date_created, 1)
        self.assertEqual(task.date_modified, 2)
        get_mock.assert_called_with(
            '%s/geo/1/tasks/test_task_1' % (url_prefix,),
            headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                     'Authorization':
                         'GEO IY3487E2J6ZHFOW5A7P5:'\
                         'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='},
            data={},
            params={},
            timeout=10,
            verify=False)

    @patch.object(requests, 'get')
    def test_get_results(self, get_mock):
        get_mock.__name__ = 'get'
        self.get_results_helper(get_mock, self.client._get_results,
            'test_task_1')

    @patch.object(snowfloat.client.Client, '_get_results')
    @patch.object(snowfloat.client.Client, '_get_task')
    @patch.object(snowfloat.client.Client, '_add_tasks')
    def test_execute_tasks(self, _add_tasks_mock, _get_task_mock,
        _get_results_mock):
        task1 = Mock()
        task1.uuid = 'test_task_1'
        task2 = Mock()
        task2.uuid = 'test_task_2'
        _add_tasks_mock.return_value = [task1, task2]
        t1 = Mock()
        t1.state = 'success'
        t2 = Mock()
        t2.state = 'success'
        _get_task_mock.side_effect = [t1, t2]
        result1 = Mock()
        result1.tag = json.dumps('test_result_1')
        result2 = Mock()
        result2.tag = json.dumps('test_result_2')
        _get_results_mock.side_effect = [[result1], [result2]]
        tasks = [snowfloat.task.Task(
                    operation='test_operation_1',
                    task_filter={'layer_uuid_exact': 'test_layer_1'}),
                 snowfloat.task.Task(
                    operation='test_operation_2',
                    task_filter={'layer_uuid_exact': 'test_layer_2'})]
        r = self.client.execute_tasks(tasks)
        self.assertListEqual(r, [['test_result_1',], ['test_result_2',]])
        self.execute_tasks_add_tasks_helper(_add_tasks_mock)

    def execute_tasks_add_tasks_helper(self, add_tasks_mock):
        d = [
            {'operation': 'test_operation_1',
             'filter': {'layer__uuid__exact': 'test_layer_1'},
             'spatial': {},
             'extras': {}},
            {'operation': 'test_operation_2',
             'filter': {'layer__uuid__exact': 'test_layer_2'},
             'spatial': {},
             'extras': {}},
        ]
        add_tasks_mock.assert_called_with(d)

    @patch.object(snowfloat.client.Client, '_get_results')
    @patch.object(snowfloat.client.Client, '_get_task')
    @patch.object(snowfloat.client.Client, '_add_tasks')
    def test_execute_tasks_task_failure(self, _add_tasks_mock, _get_task_mock,
            _get_results_mock):
        task1 = Mock()
        task1.uuid = 'test_task_1'
        task2 = Mock()
        task2.uuid = 'test_task_2'
        _add_tasks_mock.return_value = [task1, task2]
        t1 = Mock()
        t1.state = 'success'
        t2 = Mock()
        t2.state = 'failure'
        t2.reason = 'test_reason'
        _get_task_mock.side_effect = [t1, t2]
        result1 = Mock()
        result1.tag = json.dumps('test_result_1')
        result2 = Mock()
        result2.tag = json.dumps('test_result_2')
        _get_results_mock.side_effect = [[result1], [result2]]
        tasks = [snowfloat.task.Task(
                    operation='test_operation_1',
                    task_filter={'layer_uuid_exact': 'test_layer_1'}),
                 snowfloat.task.Task(
                    operation='test_operation_2',
                    task_filter={'layer_uuid_exact': 'test_layer_2'})]
        r = self.client.execute_tasks(tasks)
        self.assertListEqual(r, [['test_result_1',], {'error': 'test_reason'}])
        self.execute_tasks_add_tasks_helper(_add_tasks_mock)
        self.assertEqual(_get_task_mock.call_args_list,
            [call(task1.uuid), call(task2.uuid)])
        self.assertEqual(_get_results_mock.call_args_list,
            [call(task1.uuid),])

    @patch.object(snowfloat.client.Client, '_get_results')
    @patch.object(snowfloat.client.Client, '_get_task')
    @patch.object(snowfloat.client.Client, '_add_tasks')
    def test_execute_tasks_task_wait(self, _add_tasks_mock, _get_task_mock,
            _get_results_mock):
        task1 = Mock()
        task1.uuid = 'test_task_1'
        task2 = Mock()
        task2.uuid = 'test_task_2'
        _add_tasks_mock.return_value = [task1, task2]
        t1 = Mock()
        t1.state = 'success'
        t2 = Mock()
        t2.state = 'started'
        t3 = Mock()
        t3.state = 'success'
        _get_task_mock.side_effect = [t1, t2, t3]
        result1 = Mock()
        result1.tag = json.dumps('test_result_1')
        result2 = Mock()
        result2.tag = json.dumps('test_result_2')
        _get_results_mock.side_effect = [[result1], [result2]]
        tasks = [snowfloat.task.Task(
                    operation='test_operation_1',
                    task_filter={'layer_uuid_exact': 'test_layer_1'}),
                 snowfloat.task.Task(
                    operation='test_operation_2',
                    task_filter={'layer_uuid_exact': 'test_layer_2'})]
        r = self.client.execute_tasks(tasks, interval=0.1)
        self.assertListEqual(r, [['test_result_1',], ['test_result_2',]])
        self.execute_tasks_add_tasks_helper(_add_tasks_mock)
        self.assertEqual(_get_task_mock.call_args_list,
            [call(task1.uuid), call(task2.uuid), call(task2.uuid)])
        self.assertEqual(_get_results_mock.call_args_list,
            [call(task1.uuid), call(task2.uuid)])

    @patch.object(snowfloat.client.Client, '_get_results')
    @patch.object(snowfloat.client.Client, '_get_task')
    @patch.object(snowfloat.client.Client, '_add_tasks')
    def test_execute_tasks_task_request_error(self, _add_tasks_mock,
            _get_task_mock, _get_results_mock):
        task1 = Mock()
        task1.uuid = 'test_task_1'
        task2 = Mock()
        task2.uuid = 'test_task_2'
        _add_tasks_mock.return_value = [task1, task2]
        t1 = Mock()
        t1.state = 'success'
        _get_task_mock.side_effect = [t1,
            snowfloat.errors.RequestError(500, 0, '', '')]
        result1 = Mock()
        result1.tag = json.dumps('test_result_1')
        result2 = Mock()
        result2.tag = json.dumps('test_result_2')
        _get_results_mock.side_effect = [[result1], [result2]]
        tasks = [snowfloat.task.Task(
                    operation='test_operation_1',
                    task_filter={'layer_uuid_exact': 'test_layer_1'}),
                 snowfloat.task.Task(
                    operation='test_operation_2',
                    task_filter={'layer_uuid_exact': 'test_layer_2'})]
        r = self.client.execute_tasks(tasks)
        self.assertListEqual(r, [['test_result_1',], None])
        self.execute_tasks_add_tasks_helper(_add_tasks_mock)
        self.assertEqual(_get_task_mock.call_args_list,
            [call(task1.uuid), call(task2.uuid)])
        self.assertEqual(_get_results_mock.call_args_list,
            [call(task1.uuid),])


class LayerTests(Tests):

    def setUp(self):
        self.layer = snowfloat.layer.Layer(
            name='test_tag_1',
            uuid='test_layer_1',
            uri='/geo/1/layers/test_layer_1',
            date_created=1,
            date_modified=2,
            num_features=3,
            num_points=6)
        Tests.setUp(self)

    @patch.object(requests, 'get')
    def test_get_features(self, get_mock):
        get_mock.__name__ = 'get'
        point = snowfloat.geometry.Point(coordinates=(1, 2, 3))
        point2 = snowfloat.geometry.Point(coordinates=[4, 5, 6])
        self.get_features_helper(get_mock, self.layer.get_features,
            field_ts_gte=1, field_ts_lte=10,
            date_created_lte='2002-12-25 00:00:00-00:00',
            query='distance_lte',
            geometry=point, distance=4, spatial_operation='intersection',
            spatial_geometry=point2, spatial_flag=True,
            order_by=('-field_ts', 'date_created'))
    
    @patch.object(requests, 'post')
    def test_add_features(self, post_mock):
        post_mock.__name__ = 'post'
        self.add_features_helper(post_mock, self.layer.add_features,
            self.features)
        self.assertEqual(self.layer.num_features, 10)
        self.assertEqual(self.layer.num_points, 34)

    @patch.object(requests, 'delete')
    def test_delete_features(self, delete_mock):
        delete_mock.__name__ = 'delete'
        self.delete_features_helper(delete_mock,
            self.layer.delete_features, field_ts_gte=1, field_ts_lte=10,
            date_created_lte='2002-12-25 00:00:00-00:00')
        self.assertEqual(self.layer.num_features, 1)
        self.assertEqual(self.layer.num_points, 5)
    
    @patch.object(requests, 'delete')
    def test_delete_feature(self, delete_mock):
        delete_mock.__name__ = 'delete'
        self.delete_feature_helper(delete_mock,
            self.layer.delete_feature, 'test_feature_1')

    @patch.object(requests, 'put')
    def test_update(self, put_mock):
        put_mock.__name__ = 'put'
        m = Mock()
        m.status_code = 200
        m.json.return_value = {}
        put_mock.return_value = m
        self.layer.update(name='test_tag')
        put_mock.assert_called_with(
            '%s/geo/1/layers/test_layer_1' % (url_prefix),
            headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                     'Content-Type': 'application/json',
                     'Authorization':
                         'GEO IY3487E2J6ZHFOW5A7P5:'\
                         'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I=',
                     'Content-Sha':
                               'n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg='},
            data=json.dumps({'name': 'test_tag'}),
            params={},
            timeout=10,
            verify=False)
        self.assertEqual(self.layer.name, 'test_tag')

    @patch.object(requests, 'delete')
    def test_delete(self, delete_mock):
        delete_mock.__name__ = 'delete'
        m = Mock()
        m.status_code = 200
        m.json.return_value = {}
        delete_mock.return_value = m
        self.layer.delete()
        delete_mock.assert_called_with(
            '%s/geo/1/layers/test_layer_1' % (url_prefix),
            headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                     'Authorization':
                         'GEO IY3487E2J6ZHFOW5A7P5:'\
                         'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='},
            data={},
            params={},
            timeout=10,
            verify=False)


class ResultsTests(Tests):
   
    task = snowfloat.task.Task(
        operation='test_operation_1',
        uuid='test_task_1',
        uri='/geo/1/tasks/test_task_1',
        task_filter={'filter': 'test_task_filter_1'},
        spatial={'spatial': 'test_task_spatial_1'},
        extras={},
        state='started',
        reason='',
        date_created=1,
        date_modified=2)

    @patch.object(requests, 'get')
    def test_get_results(self, get_mock):
        get_mock.__name__ = 'get'
        self.get_results_helper(get_mock, self.task.get_results)


class FeaturesTests(Tests):

    point = snowfloat.geometry.Point(coordinates=[1, 2, 3])
    fields = {'ts': 1, 'tag': 'test_tag'}
    uri='/geo/1/layers/test_layer_1/features/test_feature_1'
    feature = snowfloat.feature.Feature(point, fields=fields, uri=uri)

    @patch.object(requests, 'put')
    def test_update(self, put_mock):
        put_mock.__name__ = 'put'
        m = Mock()
        m.status_code = 200
        m.json.return_value = {}
        put_mock.return_value = m
        fields = {'ts': 2, 'tag': 'test_tag_1'}
        point = snowfloat.geometry.Point(coordinates=[4, 5, 6])
        self.feature.update(geometry=point, fields=fields)
        d = {'type': 'Feature',
             'geometry': {'type': 'Point',
                          'coordinates': [4, 5, 6]},
             'properties': {
                  'field_tag': 'test_tag_1',
                  'field_ts': 2,
              }}
        put_mock.assert_called_with(
            '%s/geo/1/layers/test_layer_1/'\
                'features/test_feature_1' % (url_prefix),
            headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                     'Content-Type': 'application/json',
                     'Authorization':
                         'GEO IY3487E2J6ZHFOW5A7P5:'\
                         'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I=',
                     'Content-Sha':
                               'n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg='},
            data=json.dumps(d),
            params={},
            timeout=10,
            verify=False)
        self.assertListEqual(self.feature.geometry.coordinates, [4, 5, 6])
        self.assertDictEqual(self.feature.fields,
            {'ts': 2, 'tag': 'test_tag_1'})

    @patch.object(requests, 'delete')
    def test_delete(self, delete_mock):
        delete_mock.__name__ = 'delete'
        self.delete_feature_helper(delete_mock,
            self.feature.delete)


class PolygonsTests(Tests):

    def test_polygon_not_closed(self):
        polygon = snowfloat.geometry.Polygon(
            [[[0, 0, 0], [1, 1, 0], [1, 0, 0]]])
        self.assertListEqual(polygon.coordinates,
            [[[0, 0, 0], [1, 1, 0], [1, 0, 0], [0, 0, 0]]])


class GeometriesTests(Tests):

    def test_geometry(self):
        geometry = snowfloat.geometry.Geometry([1, 2, 3])
        self.assertListEqual(geometry.coordinates, [1, 2, 3])
        self.assertIsNone(geometry.geometry_type)
        self.assertRaises(NotImplementedError,
            geometry.num_points)


class ImportDataSourceTests(Tests):
   
    @patch.object(requests, 'get')
    @patch.object(requests, 'delete')
    @patch.object(snowfloat.client.Client, 'execute_tasks')
    @patch.object(requests, 'post')
    def test_import_geospatial_data(self, post_mock, execute_tasks_mock,
            delete_mock, get_mock):
        get_mock.__name__ = 'get'
        delete_mock.__name__ = 'delete'
        post_mock.__name__ = 'post'
        r = {'uuid': 'test_blob_uuid'}
        m1 = Mock()
        m1.status_code = 200
        m1.json.return_value = r
        post_mock.return_value = m1
        m2 = Mock()
        m2.status_code = 200
        m2.json.side_effect = snowfloat.errors.RequestError(status=500,
            code=None, message=None, more=None)
        delete_mock.return_value = m2
        m3 = Mock()
        m3.status_code = 200
        m3.json.side_effect = [
            {'uuid': 'test_blob_uuid', 'state': 'started'},
            {'uuid': 'test_blob_uuid', 'state': 'success'}]
        get_mock.return_value = m3
        execute_tasks_mock.return_value = [['test_result',]]
        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.close()
        srs = {'type': 'EPSG',
               'properties': {'code': 4326, 'dim': 3}}
        res = self.client.import_geospatial_data(tf.name, srs,
            state_check_interval=0.1)
        self.assertEqual(res, 'test_result')
        self.import_geospatial_data_helper(post_mock, get_mock, delete_mock,
            execute_tasks_mock)
        os.remove(tf.name)

    @patch.object(requests, 'get')
    @patch.object(requests, 'delete')
    @patch.object(snowfloat.client.Client, 'execute_tasks')
    @patch.object(requests, 'post')
    def test_import_geospatial_data_task_error(self, post_mock,
            execute_tasks_mock, delete_mock, get_mock):
        get_mock.__name__ = 'get'
        delete_mock.__name__ = 'delete'
        post_mock.__name__ = 'post'
        r = {'uuid': 'test_blob_uuid'}
        m1 = Mock()
        m1.status_code = 200
        m1.json.return_value = r
        post_mock.return_value = m1
        m2 = Mock()
        m2.status_code = 200
        m2.json.side_effect = snowfloat.errors.RequestError(status=500,
            code=None, message=None, more=None)
        delete_mock.return_value = m2
        m3 = Mock()
        m3.status_code = 200
        m3.json.side_effect = [
            {'uuid': 'test_blob_uuid', 'state': 'started'},
            {'uuid': 'test_blob_uuid', 'state': 'success'}]
        get_mock.return_value = m3
        execute_tasks_mock.return_value = [{'error': 'test_error'},]
        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.close()
        srs = {'type': 'EPSG',
               'properties': {'code': 4326, 'dim': 3}}
        self.assertRaises(snowfloat.errors.RequestError,
            self.client.import_geospatial_data, tf.name, srs,
            state_check_interval=0.1)
        self.import_geospatial_data_helper(post_mock, get_mock, delete_mock,
            execute_tasks_mock)
        os.remove(tf.name)

    @patch.object(requests, 'get')
    @patch.object(requests, 'delete')
    @patch.object(snowfloat.client.Client, 'execute_tasks')
    @patch.object(requests, 'post')
    def test_import_geospatial_data_blob_state_failure(self, post_mock,
            execute_tasks_mock, delete_mock, get_mock):
        get_mock.__name__ = 'get'
        delete_mock.__name__ = 'delete'
        post_mock.__name__ = 'post'
        r = {'uuid': 'test_blob_uuid'}
        m1 = Mock()
        m1.status_code = 200
        m1.json.return_value = r
        post_mock.return_value = m1
        m2 = Mock()
        m2.status_code = 200
        m2.json.return_value = {}
        delete_mock.return_value = m2
        m3 = Mock()
        m3.status_code = 200
        m3.json.side_effect = [
            {'uuid': 'test_blob_uuid', 'state': 'started'},
            {'uuid': 'test_blob_uuid', 'state': 'failure'}]
        get_mock.return_value = m3
        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.close()
        self.assertRaises(snowfloat.errors.RequestError,
            self.client.import_geospatial_data, tf.name,
            state_check_interval=0.1)
        self.import_geospatial_data_helper(post_mock, get_mock, delete_mock,
            execute_tasks_mock, validate_execute_tasks=False)
        os.remove(tf.name)

    def import_geospatial_data_helper(self, post_mock, get_mock, delete_mock,
            execute_tasks_mock, validate_execute_tasks=True):
        # validate post call
        ca = post_mock.call_args
        self.assertEqual(ca[0][0], '%s/geo/1/blobs' % (url_prefix,))
        self.assertDictEqual(ca[1]['headers'],
            {'Authorization':
                 'GEO IY3487E2J6ZHFOW5A7P5:'\
                 'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I=',
             'Content-Sha':
                 'n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg=',
             'Content-Type': 'application/octet-stream',
             'Date': 'Sat, 08 Jun 2013 22:12:05 GMT'})
        self.assertDictEqual(ca[1]['params'], {})
        self.assertEqual(ca[1]['timeout'], 10)
        self.assertEqual(ca[1]['verify'], False)
        # validate get call
        ca = get_mock.call_args
        self.assertEqual(ca[0][0],
            '%s/geo/1/blobs/test_blob_uuid' % (url_prefix,))
        self.assertDictEqual(ca[1]['headers'],
            {'Authorization':
                 'GEO IY3487E2J6ZHFOW5A7P5:'\
                 'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I=',
             'Date': 'Sat, 08 Jun 2013 22:12:05 GMT'})
        self.assertDictEqual(ca[1]['params'], {})
        self.assertEqual(ca[1]['timeout'], 10)
        self.assertEqual(ca[1]['verify'], False)
        # validate execute_tasks call
        if validate_execute_tasks:
            ca = execute_tasks_mock.call_args
            task = ca[0][0][0]
            self.assertEqual(task.operation, 'import_geospatial_data')
            self.assertDictEqual(task.extras, {'blob_uuid': 'test_blob_uuid',
                'srs': {'type': 'EPSG',
                        'properties': {'code': 4326, 'dim': 3}}})
        else:
            self.assertFalse(execute_tasks_mock.called)
        delete_mock.assert_called_with(
            '%s/geo/1/blobs/test_blob_uuid' % (url_prefix),
            headers={'Date': 'Sat, 08 Jun 2013 22:12:05 GMT',
                     'Authorization':
                         'GEO IY3487E2J6ZHFOW5A7P5:'\
                         'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I='},
            data={},
            params={},
            timeout=10,
            verify=False)
    

class RequestErrorTests(unittest.TestCase):

    def test_request_error(self):
        req = snowfloat.errors.RequestError(500, 1, 'test_message',
            'test_more')
        self.assertEqual(str(req),
            'RequestError(status=500, code=1, message=test_message, ' \
            'more=test_more)')


class RequestTests(unittest.TestCase):

    def setUp(self):
        snowfloat.settings.HOST = 'api.snowfloat.com:443'
    
    def test_get_hmac_sha(self):
        msg = 'test_msg'
        private_key = 'test_private_key'
        res = snowfloat.request._get_hmac_sha(msg, private_key)
        self.assertEqual(res, 'w5YfAjs+VUh79G1jYgHZFWLA4w9W+MNDRX/9z8kFJKY=')

    def test_get_sha_file(self):
        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.write(''.join('a' for e in xrange(10000)))
        tf.close()
        tf = open(tf.name)
        res = snowfloat.request._get_sha(tf)
        self.assertEqual(res, 'J90fYbhntqD26dikHEMjHeUhB+U65CTej4R7gh20txE=')
        tf.close()

    def test_get_sha_str(self):
        res = snowfloat.request._get_sha('test_message')
        self.assertEqual(res, 'O3SR3AFqwaCy4CNyQCyGH6+kWSlAh+fL4J9wTVgtkx8=')

    def test_format_url_https(self):
        res = snowfloat.request._format_url('/test_uri')
        self.assertEqual(res, 'https://api.snowfloat.com:443/test_uri')

    def test_format_url_http(self):
        snowfloat.settings.HOST = 'api.snowfloat.com:80'
        res = snowfloat.request._format_url('/test_uri')
        self.assertEqual(res, 'http://api.snowfloat.com:80/test_uri')

    def test_format_params_query_distance(self):
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

    @patch.object(snowfloat.request, '_get_headers')
    @patch.object(requests, 'get')
    def test_send_get(self, get_mock, get_headers_mock):
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
            '%s/test_uri' % (url_prefix),
            headers={'header_1': 'test_header_1',
                     'header_2': 'test_header_2'},
            params={'param': 'test_param'},
            data={},
            timeout=10,
            verify=False)


if __name__ == "__main__":
    unittest.main()
