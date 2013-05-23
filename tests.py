import json
import os
import tempfile
import unittest

from mock import Mock, patch, call
import requests

import snowfloat.auth
import snowfloat.client
import snowfloat.errors
import snowfloat.geometry
import snowfloat.settings
import snowfloat.task

class Tests(unittest.TestCase):
   
    url_prefix = 'http://%s' % (snowfloat.settings.HOST,)

    def setUp(self):
        snowfloat.settings.HTTP_RETRY_INTERVAL = 0.1
        snowfloat.auth.session_uuid = 'test_session_uuid'
        self.client = snowfloat.client.Client()

        self.features = []
        geometry = snowfloat.geometry.Point(coordinates=[1, 2, 3])
        fields = {'ts': 4, 'tag': 'test_tag_1'}
        feature = snowfloat.feature.Feature(geometry, fields=fields)
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
                            'ts_created': 5,
                            'ts_modified': 6,
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
                            'ts_created': 12,
                            'ts_modified': 13,
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
                            'ts_created': 22,
                            'ts_modified': 23,
                            'spatial': {'type': 'Point',
                                        'coordinates': [10, 11, 12]}}

                        }]}}
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
        features = [e for e in method(*args, **kwargs)]
        feature = features[0]
        self.assertListEqual(feature.geometry.coordinates, [1, 2, 3])
        self.assertEqual(feature.fields['ts'], 4)
        self.assertEqual(feature.fields['tag'], 'test_tag_1')
        self.assertEqual(feature.uri,
            '/geo/1/layers/test_layer_1/features/test_point_1')
        self.assertEqual(feature.uuid, 'test_point_1')
        self.assertEqual(feature.ts_created, 5)
        self.assertEqual(feature.ts_modified, 6)
        self.assertEqual(feature.spatial.geometry_type, 'Point')
        self.assertListEqual(feature.spatial.coordinates, [4, 5, 6])
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
        self.assertEqual(feature.ts_created, 12)
        self.assertEqual(feature.ts_modified, 13)
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
        self.assertEqual(feature.ts_created, 22)
        self.assertEqual(feature.ts_modified, 23)
        self.assertEqual(feature.spatial.geometry_type, 'Point')
        self.assertListEqual(feature.spatial.coordinates, [10, 11, 12])
        distance_lookup = {'type': 'Point',
                           'coordinates': [1, 2, 3],
                           'properties': {'distance': 4}}
        spatial_geometry = {'type': 'Point',
                            'coordinates': [4, 5, 6]}
        self.assertEqual(method_mock.call_args_list,
            [call('%s/geo/1/layers/test_layer_1/features'
                    % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_uuid'},
                  params={'field__ts__gte': 1,
                          'field__ts__lte': 10,
                          'geometry__distance_lte':
                            json.dumps(distance_lookup),
                          'spatial_operation': 'intersection',
                          'spatial_geometry':
                            json.dumps(spatial_geometry),
                          'spatial_flag': True},
                  data={},
                  timeout=10,
                  verify=False),
             call('%s/geo/1/layers/test_layer_1/features?page=1'\
                    '&page_size=2'
                  % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_uuid'},
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
                   'ts_created': 5,
                   'ts_modified': 6}
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
                   'ts_created': 12,
                   'ts_modified': 13}
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
                    'ts_created': 22,
                    'ts_modified': 23}
                }]}
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
        self.assertEqual(feature.ts_created, 5)
        self.assertEqual(feature.ts_modified, 6)
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
        self.assertEqual(feature.ts_created, 12)
        self.assertEqual(feature.ts_modified, 13)
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
        self.assertEqual(feature.ts_created, 22)
        self.assertEqual(feature.ts_modified, 23)
        for i in range(3):
            del r['features'][i]['id']
            del r['features'][i]['properties']['uri']
            del r['features'][i]['properties']['ts_created']
            del r['features'][i]['properties']['ts_modified']
        self.assertEqual(method_mock.call_args_list,
            [call('%s/geo/1/layers/test_layer_1/features'
                    % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_uuid'},
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
            '%s/geo/1/layers/test_layer_1/features' % (self.url_prefix),
            headers={'X-Session-ID': 'test_session_uuid'},
            params={'field__ts__gte': 1, 'field__ts__lte': 10},
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
                'features/test_feature_1' % (self.url_prefix),
            headers={'X-Session-ID': 'test_session_uuid'},
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
                  'ts_created': 1,
                  'ts_modified': 2,
                },
                {
                  'uuid': 'test_result_2',
                  'uri': '/geo/1/tasks/test_task_1/results/test_result_2',
                  'tag': 'test_tag_2',
                  'ts_created': 3,
                  'ts_modified': 4,
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
        self.assertEqual(result.ts_created, 1)
        self.assertEqual(result.ts_modified, 2)
        result = results[1]
        self.assertEqual(result.uuid, 'test_result_2')
        self.assertEqual(result.uri,
            '/geo/1/tasks/test_task_1/results/test_result_2')
        self.assertEqual(result.tag, 'test_tag_2')
        self.assertEqual(result.ts_created, 3)
        self.assertEqual(result.ts_modified, 4)
        self.assertEqual(method_mock.call_args_list,
            [call('%s/geo/1/tasks/test_task_1/results' % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_uuid'},
                  data={},
                  params={},
                  timeout=10,
                  verify=False),
             call('%s/geo/1/tasks/test_task_1/results?page=1&page_size=2'
                  % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_uuid'},
                  data={},
                  params={},
                  timeout=10,
                  verify=False)])


class ClientTests(Tests):
   
    @patch.object(requests, 'get')
    def test_get_layers(self, get_mock):
        r1 = {
                'next_page_uri': '/geo/1/layers?page=1&page_size=2',
                'total': 2,
                'layers': [{'name': 'test_tag_1',
                            'uri': '/geo/1/layers/test_layer_1',
                            'uuid': 'test_layer_1',
                            'ts_created': 1,
                            'ts_modified': 2,
                            'num_features': 10,
                            'num_points': 20
                           },
                           {'name': 'test_tag_2',
                            'uri': '/geo/1/layers/test_layer_2',
                            'uuid': 'test_layer_2',
                            'ts_created': 3,
                            'ts_modified': 4,
                            'num_features': 11,
                            'num_points': 21
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
        layers = [e for e in self.client.get_layers()]
        self.assertEqual(layers[0].name, 'test_tag_1')
        self.assertEqual(layers[0].uri,
            '/geo/1/layers/test_layer_1')
        self.assertEqual(layers[0].uuid, 'test_layer_1')
        self.assertEqual(layers[0].ts_created, 1)
        self.assertEqual(layers[0].ts_modified, 2)
        self.assertEqual(layers[0].num_features, 10)
        self.assertEqual(layers[0].num_points, 20)
        self.assertEqual(layers[1].name, 'test_tag_2')
        self.assertEqual(layers[1].uri,
            '/geo/1/layers/test_layer_2')
        self.assertEqual(layers[1].uuid, 'test_layer_2')
        self.assertEqual(layers[1].ts_created, 3)
        self.assertEqual(layers[1].ts_modified, 4)
        self.assertEqual(layers[1].num_features, 11)
        self.assertEqual(layers[1].num_points, 21)
        self.assertEqual(get_mock.call_args_list,
            [call('%s/geo/1/layers' % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_uuid'},
                  data={},
                  params={},
                  timeout=10,
                  verify=False),
             call('%s/geo/1/layers?page=1&page_size=2' % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_uuid'},
                  data={},
                  params={},
                  timeout=10,
                  verify=False)])


    @patch.object(requests, 'get')
    def test_get_layers_requests_get_error(self, get_mock):
        r = {'status': 400,
             'code': 1,
             'message': 'test_message',
             'more': 'test_more'
            }
        m = Mock()
        m.status_code = 400
        m.json.return_value = r
        get_mock.return_value = m
        self.assertRaises(snowfloat.errors.RequestError,
            r = self.client.get_layers())

    @patch.object(requests, 'get')
    def test_get_layers_status_400(self, get_mock):
        get_mock.side_effect = Exception('error')
        self.assertRaises(snowfloat.errors.RequestError,
            r = self.client.get_layers())

    @patch.object(requests, 'post')
    def test_add_layers(self, post_mock):
        r = [{'name': 'test_tag_1',
              'uri': '/geo/1/layers/test_layer_1',
              'uuid': 'test_layer_1',
              'ts_created': 1,
              'ts_modified': 2,
              'num_features': 0,
              'num_points': 0
             },
             {'name': 'test_tag_2',
              'uri': '/geo/1/layers/test_layer_2',
              'uuid': 'test_layer_2',
              'ts_created': 3,
              'ts_modified': 4,
              'num_features': 0,
              'num_points': 0
             }]
        m = Mock()
        m.status_code = 200
        m.json.return_value = r
        post_mock.return_value = m
        layers = [snowfloat.layer.Layer(name='test_tag_1'),
                      snowfloat.layer.Layer(name='test_tag_2')]
        layers = self.client.add_layers(layers)
        self.assertEqual(layers[0].name, 'test_tag_1')
        self.assertEqual(layers[0].uri,
            '/geo/1/layers/test_layer_1')
        self.assertEqual(layers[0].uuid, 'test_layer_1')
        self.assertEqual(layers[0].ts_created, 1)
        self.assertEqual(layers[0].ts_modified, 2)
        self.assertEqual(layers[0].num_features, 0)
        self.assertEqual(layers[0].num_points, 0)
        self.assertEqual(layers[1].name, 'test_tag_2')
        self.assertEqual(layers[1].uri,
            '/geo/1/layers/test_layer_2')
        self.assertEqual(layers[1].uuid, 'test_layer_2')
        self.assertEqual(layers[1].ts_created, 3)
        self.assertEqual(layers[1].ts_modified, 4)
        self.assertEqual(layers[1].num_features, 0)
        self.assertEqual(layers[1].num_points, 0)
        self.assertEqual(post_mock.call_args_list,
            [call('%s/geo/1/layers' % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_uuid'},
                  data=json.dumps([{'name': 'test_tag_1'},
                                   {'name': 'test_tag_2'}]),
                  params={},
                  timeout=10,
                  verify=False)])

    @patch.object(requests, 'delete')
    def test_delete_layers(self, delete_mock):
        m = Mock()
        m.status_code = 200
        m.json.return_value = {}
        delete_mock.return_value = m
        self.client.delete_layers()
        delete_mock.assert_called_with(
            '%s/geo/1/layers' % (self.url_prefix),
            headers={'X-Session-ID': 'test_session_uuid'},
            data={},
            params={},
            timeout=10,
            verify=False)

    @patch.object(requests, 'get')
    def test_get_features(self, get_mock):
        point = snowfloat.geometry.Point(coordinates=[1, 2, 3])
        point2 = snowfloat.geometry.Point(coordinates=[4, 5, 6])
        self.get_features_helper(get_mock, self.client.get_features,
            'test_layer_1', field_ts_gte=1, field_ts_lte=10,
            query='distance_lte',
            geometry=point, distance=4, spatial_operation='intersection',
            spatial_geometry=point2, spatial_flag=True)

    @patch.object(requests, 'post')
    def test_add_features(self, post_mock):
        self.add_features_helper(post_mock, self.client.add_features,
            'test_layer_1', self.features)

    @patch.object(requests, 'delete')
    def test_delete_features(self, delete_mock):
        self.delete_features_helper(delete_mock,
            self.client.delete_features,
            'test_layer_1', field_ts_gte=1, field_ts_lte=10)

    @patch.object(requests, 'post')
    def test_login(self, post_mock):
        r = {'more': 'test_session_2'}
        m = Mock()
        m.status_code = 200
        m.json.return_value = r
        post_mock.return_value = m
        self.client.login('test_user', 'test_key')
        self.assertEqual(snowfloat.auth.session_uuid, 'test_session_2')

    @patch.object(requests, 'post')
    def test_add_tasks(self, post_mock):
        r = [{'operation': 'test_operation_1',
              'task_filter': 'test_task_filter_1',
              'uri': '/geo/1/tasks/test_task_1',
              'uuid': 'test_task_1',
              'state': 'started',
              'extras': {'extra': 'test_extra_1'},
              'reason': 'test_reason_1',
              'ts_created': 1,
              'ts_modified': 2
             },
             {'operation': 'test_operation_2',
              'task_filter': 'test_task_filter_2',
              'uri': '/geo/1/tasks/test_task_2',
              'uuid': 'test_task_2',
              'state': 'started',
              'extras': {'extra': 'test_extra_2'},
              'reason': 'test_reason_2',
              'ts_created': 3,
              'ts_modified': 4
             }]
        m = Mock()
        m.status_code = 200
        m.json.return_value = r
        post_mock.return_value = m
        tasks = [{'operation': 'test_operation_1'},
                 {'operation': 'test_operation_2'}]
        tasks = self.client._add_tasks(tasks)
        self.assertEqual(tasks[0].operation, 'test_operation_1')
        self.assertEqual(tasks[0].task_filter, 'test_task_filter_1')
        self.assertEqual(tasks[0].uuid, 'test_task_1')
        self.assertEqual(tasks[0].state, 'started')
        self.assertDictEqual(tasks[0].extras, {'extra': 'test_extra_1'})
        self.assertEqual(tasks[0].reason, 'test_reason_1')
        self.assertEqual(tasks[0].uri, '/geo/1/tasks/test_task_1')
        self.assertEqual(tasks[0].ts_created, 1)
        self.assertEqual(tasks[0].ts_modified, 2)
        self.assertEqual(tasks[1].operation, 'test_operation_2')
        self.assertEqual(tasks[1].task_filter, 'test_task_filter_2')
        self.assertEqual(tasks[1].uuid, 'test_task_2')
        self.assertEqual(tasks[1].state, 'started')
        self.assertDictEqual(tasks[1].extras, {'extra': 'test_extra_2'})
        self.assertEqual(tasks[1].reason, 'test_reason_2')
        self.assertEqual(tasks[1].uri, '/geo/1/tasks/test_task_2')
        self.assertEqual(tasks[1].ts_created, 3)
        self.assertEqual(tasks[1].ts_modified, 4)

    @patch.object(requests, 'get')
    def test_get_task(self, get_mock):
        r = {'operation': 'test_operation_1',
             'task_filter': 'test_task_filter_1',
             'uri': '/geo/1/tasks/test_task_1',
             'uuid': 'test_task_1',
             'state': 'started',
             'extras': {'extra': 'test_extra_1'},
             'reason': 'test_reason_1',
             'ts_created': 1,
             'ts_modified': 2
            }
        m = Mock()
        m.status_code = 200
        m.json.return_value = r
        get_mock.return_value = m
        task = self.client._get_task('test_task_1')
        self.assertEqual(task.operation, 'test_operation_1')
        self.assertEqual(task.task_filter, 'test_task_filter_1')
        self.assertEqual(task.uri, '/geo/1/tasks/test_task_1')
        self.assertEqual(task.uuid, 'test_task_1')
        self.assertEqual(task.state, 'started')
        self.assertDictEqual(task.extras, {'extra': 'test_extra_1'})
        self.assertEqual(task.reason, 'test_reason_1')
        self.assertEqual(task.ts_created, 1)
        self.assertEqual(task.ts_modified, 2)
        get_mock.assert_called_with(
            '%s/geo/1/tasks/test_task_1' % (self.url_prefix,),
            headers={'X-Session-ID': 'test_session_uuid'},
            data={},
            params={},
            timeout=10,
            verify=False)

    @patch.object(requests, 'get')
    def test_get_results(self, get_mock):
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
                    layer_uuid='test_layer_1'),
                 snowfloat.task.Task(
                    operation='test_operation_2',
                    layer_uuid='test_layer_2')]
        r = self.client.execute_tasks(tasks)
        self.assertListEqual(r, [['test_result_1',], ['test_result_2',]])
        d = [
            {'operation': 'test_operation_1',
             'layer__uuid': 'test_layer_1'},
            {'operation': 'test_operation_2',
             'layer__uuid': 'test_layer_2'}
        ]
        _add_tasks_mock.assert_called_with(d)
        self.assertEqual(_get_task_mock.call_args_list,
            [call(task1.uuid), call(task2.uuid)])
        self.assertEqual(_get_results_mock.call_args_list,
            [call(task1.uuid), call(task2.uuid)])

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
                    layer_uuid= ['test_layer_1', 'test_layer_1b']),
                 snowfloat.task.Task(
                    operation='test_operation_2',
                    layer_uuid='test_layer_2')]
        r = self.client.execute_tasks(tasks)
        self.assertListEqual(r, [['test_result_1',], {'error': 'test_reason'}])
        d = [
            {'operation': 'test_operation_1',
             'layer__uuid__in': ['test_layer_1', 'test_layer_1b']},
            {'operation': 'test_operation_2',
             'layer__uuid': 'test_layer_2'}
        ]
        _add_tasks_mock.assert_called_with(d)
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
                    layer_uuid='test_layer_1'),
                 snowfloat.task.Task(
                    operation='test_operation_2',
                    layer_uuid='test_layer_2')]
        r = self.client.execute_tasks(tasks, interval=0.1)
        self.assertListEqual(r, [['test_result_1',], ['test_result_2',]])
        d = [
            {'operation': 'test_operation_1',
             'layer__uuid': 'test_layer_1'},
            {'operation': 'test_operation_2',
             'layer__uuid': 'test_layer_2'}
        ]
        _add_tasks_mock.assert_called_with(d)
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
                    layer_uuid='test_layer_1'),
                 snowfloat.task.Task(
                    operation='test_operation_2',
                    layer_uuid='test_layer_2')]
        r = self.client.execute_tasks(tasks)
        self.assertListEqual(r, [['test_result_1',], None])
        d = [
            {'operation': 'test_operation_1',
             'layer__uuid': 'test_layer_1'},
            {'operation': 'test_operation_2',
             'layer__uuid': 'test_layer_2'}
        ]
        _add_tasks_mock.assert_called_with(d)
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
            ts_created=1,
            ts_modified=2,
            num_features=3,
            num_points=6)
        Tests.setUp(self)

    @patch.object(requests, 'get')
    def test_get_features(self, get_mock):
        point = snowfloat.geometry.Point(coordinates=(1, 2, 3))
        point2 = snowfloat.geometry.Point(coordinates=[4, 5, 6])
        self.get_features_helper(get_mock, self.layer.get_features,
            field_ts_gte=1, field_ts_lte=10, query='distance_lte',
            geometry=point, distance=4, spatial_operation='intersection',
            spatial_geometry=point2, spatial_flag=True)
    
    @patch.object(requests, 'post')
    def test_add_features(self, post_mock):
        self.add_features_helper(post_mock, self.layer.add_features,
            self.features)
        self.assertEqual(self.layer.num_features, 6)
        self.assertEqual(self.layer.num_points, 19)

    @patch.object(requests, 'delete')
    def test_delete_features(self, delete_mock):
        self.delete_features_helper(delete_mock,
            self.layer.delete_features, field_ts_gte=1, field_ts_lte=10)
        self.assertEqual(self.layer.num_features, 1)
        self.assertEqual(self.layer.num_points, 5)
    
    @patch.object(requests, 'delete')
    def test_delete_feature(self, delete_mock):
        self.delete_feature_helper(delete_mock,
            self.layer.delete_feature, 'test_feature_1')

    @patch.object(requests, 'put')
    def test_update(self, put_mock):
        m = Mock()
        m.status_code = 200
        m.json.return_value = {}
        put_mock.return_value = m
        self.layer.update(name='test_tag')
        put_mock.assert_called_with(
            '%s/geo/1/layers/test_layer_1' % (self.url_prefix),
            headers={'X-Session-ID': 'test_session_uuid'},
            data=json.dumps({'name': 'test_tag'}),
            params={},
            timeout=10,
            verify=False)
        self.assertEqual(self.layer.name, 'test_tag')

    @patch.object(requests, 'delete')
    def test_delete(self, delete_mock):
        m = Mock()
        m.status_code = 200
        m.json.return_value = {}
        delete_mock.return_value = m
        self.layer.delete()
        delete_mock.assert_called_with(
            '%s/geo/1/layers/test_layer_1' % (self.url_prefix),
            headers={'X-Session-ID': 'test_session_uuid'},
            data={},
            params={},
            timeout=10,
            verify=False)


class ResultsTests(Tests):
   
    task = snowfloat.task.Task(
        operation='test_operation_1',
        uuid='test_task_1',
        uri='/geo/1/tasks/test_task_1',
        task_filter='test_task_filter_1',
        extras={},
        state='started',
        reason='',
        ts_created=1,
        ts_modified=2)

    @patch.object(requests, 'get')
    def test_get_results(self, get_mock):
        self.get_results_helper(get_mock, self.task.get_results)


class FeaturesTests(Tests):

    point = snowfloat.geometry.Point(coordinates=[1, 2, 3])
    fields = {'ts': 1, 'tag': 'test_tag'}
    uri='/geo/1/layers/test_layer_1/features/test_feature_1'
    feature = snowfloat.feature.Feature(point, fields=fields, uri=uri)

    @patch.object(requests, 'put')
    def test_update(self, put_mock):
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
                'features/test_feature_1' % (self.url_prefix),
            headers={'X-Session-ID': 'test_session_uuid'},
            data=json.dumps(d),
            params={},
            timeout=10,
            verify=False)
        self.assertListEqual(self.feature.geometry.coordinates, [4, 5, 6])
        self.assertDictEqual(self.feature.fields,
            {'ts': 2, 'tag': 'test_tag_1'})

    @patch.object(requests, 'delete')
    def test_delete(self, delete_mock):
        self.delete_feature_helper(delete_mock,
            self.feature.delete)


class PointsTests(Tests):

    def test_point_no_z(self):
        point = snowfloat.geometry.Point([1, 2])
        self.assertListEqual(point.coordinates, [1, 2, 0])


class PolygonsTests(Tests):

    def test_polygon_not_closed(self):
        polygon = snowfloat.geometry.Polygon(
            [[[0, 0, 0], [1, 1, 0], [1, 0, 0]]])
        self.assertListEqual(polygon.coordinates,
            [[[0, 0, 0], [1, 1, 0], [1, 0, 0], [0, 0, 0]]])


class ImportDataSourceTests(Tests):
   
    @patch.object(requests, 'delete')
    @patch.object(snowfloat.client.Client, 'execute_tasks')
    @patch.object(requests, 'post')
    def test_import_geospatial_data(self, post_mock, execute_tasks_mock,
            delete_mock):
        r = {'uuid': 'test_blob_uuid'}
        m1 = Mock()
        m1.status_code = 200
        m1.json.return_value = r
        post_mock.return_value = m1
        m2 = Mock()
        m2.status_code = 200
        m2.json.return_value = {}
        delete_mock.return_value = m2
        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.close()
        r = self.client.import_geospatial_data(tf.name)
        ca = post_mock.call_args
        self.assertEqual(ca[0][0], '%s/geo/1/blobs' % (self.url_prefix,))
        self.assertDictEqual(ca[1]['headers'],
            {'X-Session-ID': 'test_session_uuid'})
        self.assertDictEqual(ca[1]['params'], {})
        self.assertEqual(ca[1]['timeout'], 10)
        self.assertEqual(ca[1]['verify'], False)
        ca = execute_tasks_mock.call_args
        task = ca[0][0][0]
        self.assertEqual(task.operation, 'import_geospatial_data')
        self.assertDictEqual(task.extras, {'blob_uuid': 'test_blob_uuid'})
        delete_mock.assert_called_with(
        '%s/geo/1/blobs/test_blob_uuid' % (self.url_prefix),
        headers={'X-Session-ID': 'test_session_uuid'},
        data={},
        params={},
        timeout=10,
        verify=False)
        os.remove(tf.name)


if __name__ == "__main__":
    unittest.main()
