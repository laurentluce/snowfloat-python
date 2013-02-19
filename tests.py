import json
import unittest

from mock import Mock, patch, call
import requests

import snowfloat.auth
import snowfloat.client
import snowfloat.errors
import snowfloat.geometry
import snowfloat.settings

class Tests(unittest.TestCase):
   
    url_prefix = 'http://%s' % (snowfloat.settings.HOST,)

    def setUp(self):
        snowfloat.settings.HTTP_RETRY_INTERVAL = 0.1
        snowfloat.settings.HTTP_PUT_BATCH_SIZE = 2
        snowfloat.auth.session_id = 'test_session_id'
        self.client = snowfloat.client.Client()

        self.geometries = [
            snowfloat.geometry.Point(
                  coordinates=[1, 2, 3],
                  ts=4, dat='test_dat_1'),
            snowfloat.geometry.Polygon(
                  coordinates=[[[11, 12, 13],
                                [14, 15, 16],
                                [17, 18, 19],
                                [11, 12, 13]]],
                  ts=11, dat='test_dat_2'),
            snowfloat.geometry.Point(
                  coordinates=[21, 22, 23],
                  ts=21, dat='test_dat_3')
        ]

    def get_geometries_helper(self, method_mock, method, *args, **kwargs):
        r1 = {
                'next_page_uri':
                    '/geo/1/containers/test_container_1/geometries?page=1'\
                    '&page_size=2',
                'total': 2,
                'geo': {
                      'type': 'FeatureCollection',
                      'features': [
                        {'type': 'Feature',
                         'id': 'test_point_1',
                         'geometry': {'type': 'Point',
                                      'coordinates': [1, 2, 3]},
                         'properties': {
                            'uri': '/geo/1/containers/test_container_1/'\
                                'geometries/test_point_1',
                            'ts': 4,
                            'dat': 'test_dat_1',
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
                            'uri': '/geo/1/containers/test_container_1/'\
                                'geometries/test_polygon_1',
                            'ts': 11,
                            'dat': 'test_dat_2',
                            'ts_created': 12,
                            'ts_modified': 13}
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
        geometries = [e for e in method(*args, **kwargs)]
        self.assertListEqual(geometries[0].coordinates, [1, 2, 3])
        self.assertEqual(geometries[0].ts, 4)
        self.assertEqual(geometries[0].dat, 'test_dat_1')
        self.assertEqual(geometries[0].uri,
            '/geo/1/containers/test_container_1/geometries/test_point_1')
        self.assertEqual(geometries[0].id, 'test_point_1')
        self.assertEqual(geometries[0].ts_created, 5)
        self.assertEqual(geometries[0].ts_modified, 6)
        self.assertListEqual(geometries[1].coordinates, [[[11, 12, 13],
                                                        [14, 15, 16],
                                                        [17, 18, 19],
                                                        [11, 12, 13]]])
        self.assertEqual(geometries[1].ts, 11)
        self.assertEqual(geometries[1].dat, 'test_dat_2')
        self.assertEqual(geometries[1].uri,
            '/geo/1/containers/test_container_1/geometries/test_polygon_1')
        self.assertEqual(geometries[1].id, 'test_polygon_1')
        self.assertEqual(geometries[1].ts_created, 12)
        self.assertEqual(geometries[1].ts_modified, 13)
        distance_lookup = {'type': 'Point',
                           'coordinates': [1, 2, 3],
                           'properties': {'distance': 4}}
        self.assertEqual(method_mock.call_args_list,
            [call('%s/geo/1/containers/test_container_1/geometries'
                    % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_id'},
                  params={'ts__gte': 1, 'ts__lte': 10,
                        'geometry__distance_lte': json.dumps(distance_lookup)},
                  data={},
                  timeout=10,
                  verify=False),
             call('%s/geo/1/containers/test_container_1/geometries?page=1'\
                    '&page_size=2'
                  % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_id'},
                  params={},
                  data={},
                  timeout=10,
                  verify=False)])

    def add_geometries_helper(self, method_mock, method, *args, **kwargs):
        r1 = {
              'type': 'FeatureCollection',
              'features': [
                {'type': 'Feature',
                 'id': 'test_point_1',
                 'geometry': {'type': 'Point', 'coordinates': [1, 2, 3]},
                 'properties': {
                    'uri': '/geo/1/containers/test_container_1/'\
                           'geometries/test_point_1',
                    'ts': 4,
                    'dat': 'test_dat_1',
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
                    'uri': '/geo/1/containers/test_container_1/'\
                           'geometries/test_polygon_1',
                    'ts': 11,
                    'dat': 'test_dat_2',
                    'ts_created': 12,
                    'ts_modified': 13}
                }]}
        r2 = {
              'type': 'FeatureCollection',
              'features': [
                {'type': 'Feature',
                 'id': 'test_point_2',
                 'geometry': {'type': 'Point', 'coordinates': [21, 22, 23]},
                 'properties': {
                    'uri': '/geo/1/containers/test_container_1/'\
                           'geometries/test_point_2',
                    'ts': 21,
                    'dat': 'test_dat_3',
                    'ts_created': 22,
                    'ts_modified': 23}
                },
                ]}
        m1 = Mock()
        m1.status_code = 200
        m1.json.return_value = r1
        m2 = Mock()
        m2.status_code = 200
        m2.json.return_value = r2
        method_mock.side_effect = [m1, m2]
        geometries = method(*args, **kwargs)
        self.assertListEqual(geometries[0].coordinates, [1, 2, 3])
        self.assertEqual(geometries[0].ts, 4)
        self.assertEqual(geometries[0].dat, 'test_dat_1')
        self.assertEqual(geometries[0].uri,
            '/geo/1/containers/test_container_1/geometries/test_point_1')
        self.assertEqual(geometries[0].container_id, 'test_container_1')
        self.assertEqual(geometries[0].id, 'test_point_1')
        self.assertEqual(geometries[0].ts_created, 5)
        self.assertEqual(geometries[0].ts_modified, 6)
        self.assertListEqual(geometries[1].coordinates, [[[11, 12, 13],
                                                        [14, 15, 16],
                                                        [17, 18, 19],
                                                        [11, 12, 13]]])
        self.assertEqual(geometries[1].ts, 11)
        self.assertEqual(geometries[1].dat, 'test_dat_2')
        self.assertEqual(geometries[1].uri,
            '/geo/1/containers/test_container_1/geometries/test_polygon_1')
        self.assertEqual(geometries[0].container_id, 'test_container_1')
        self.assertEqual(geometries[1].id, 'test_polygon_1')
        self.assertEqual(geometries[1].ts_created, 12)
        self.assertEqual(geometries[1].ts_modified, 13)
        self.assertListEqual(geometries[2].coordinates, [21, 22, 23])
        self.assertEqual(geometries[2].ts, 21)
        self.assertEqual(geometries[2].dat, 'test_dat_3')
        self.assertEqual(geometries[2].uri,
            '/geo/1/containers/test_container_1/geometries/test_point_2')
        self.assertEqual(geometries[2].id, 'test_point_2')
        self.assertEqual(geometries[2].ts_created, 22)
        self.assertEqual(geometries[2].ts_modified, 23)
        for i in range(2):
            del r1['features'][i]['id']
            del r1['features'][i]['properties']['uri']
            del r1['features'][i]['properties']['ts_created']
            del r1['features'][i]['properties']['ts_modified']
        for i in range(1):
            del r2['features'][i]['id']
            del r2['features'][i]['properties']['uri']
            del r2['features'][i]['properties']['ts_created']
            del r2['features'][i]['properties']['ts_modified']
        self.assertEqual(method_mock.call_args_list,
            [call('%s/geo/1/containers/test_container_1/geometries'
                    % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_id'},
                  data=json.dumps(r1),
                  params={},
                  timeout=10,
                  verify=False),
             call('%s/geo/1/containers/test_container_1/geometries'
                    % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_id'},
                  data=json.dumps(r2),
                  params={},
                  timeout=10,
                  verify=False)])

    def delete_geometries_helper(self, method_mock, method, *args, **kwargs):
        m = Mock()
        m.status_code = 200
        m.json.return_value = {}
        method_mock.return_value = m
        method(*args, **kwargs)
        method_mock.assert_called_with(
            '%s/geo/1/containers/test_container_1/geometries' % (self.url_prefix),
            headers={'X-Session-ID': 'test_session_id'},
            params={'ts__gte': 1, 'ts__lte': 10},
            data={},
            timeout=10,
            verify=False)

    def delete_geometry_helper(self, method_mock, method, *args, **kwargs):
        m = Mock()
        m.status_code = 200
        m.json.return_value = {}
        method_mock.return_value = m
        method(*args, **kwargs)
        method_mock.assert_called_with(
            '%s/geo/1/containers/test_container_1/'\
                'geometries/test_geometry_1' % (self.url_prefix),
            headers={'X-Session-ID': 'test_session_id'},
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
                  'id': 'test_result_1',
                  'uri': '/geo/1/tasks/test_task_1/results/test_result_1',
                  'dat': 'test_dat_1',
                  'ts_created': 1,
                  'ts_modified': 2,
                },
                {
                  'id': 'test_result_2',
                  'uri': '/geo/1/tasks/test_task_1/results/test_result_2',
                  'dat': 'test_dat_2',
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
        self.assertEqual(result.id, 'test_result_1')
        self.assertEqual(result.uri,
            '/geo/1/tasks/test_task_1/results/test_result_1')
        self.assertEqual(result.dat, 'test_dat_1')
        self.assertEqual(result.ts_created, 1)
        self.assertEqual(result.ts_modified, 2)
        result = results[1]
        self.assertEqual(result.id, 'test_result_2')
        self.assertEqual(result.uri,
            '/geo/1/tasks/test_task_1/results/test_result_2')
        self.assertEqual(result.dat, 'test_dat_2')
        self.assertEqual(result.ts_created, 3)
        self.assertEqual(result.ts_modified, 4)
        self.assertEqual(method_mock.call_args_list,
            [call('%s/geo/1/tasks/test_task_1/results' % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_id'},
                  data={},
                  params={},
                  timeout=10,
                  verify=False),
             call('%s/geo/1/tasks/test_task_1/results?page=1&page_size=2'
                  % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_id'},
                  data={},
                  params={},
                  timeout=10,
                  verify=False)])


class ClientTests(Tests):
   
    @patch.object(requests, 'get')
    def test_get_containers(self, get_mock):
        r1 = {
                'next_page_uri': '/geo/1/containers?page=1&page_size=2',
                'total': 2,
                'containers': [{'dat': 'test_dat_1',
                            'uri': '/geo/1/containers/test_container_1',
                            'id': 'test_container_1',
                            'ts_created': 1,
                            'ts_modified': 2
                           },
                           {'dat': 'test_dat_2',
                            'uri': '/geo/1/containers/test_container_2',
                            'id': 'test_container_2',
                            'ts_created': 3,
                            'ts_modified': 4
                           }],
            }
        r2 = {
                'next_page_uri': None,
                'total': 0,
                'containers': [],
            }
        m1 = Mock()
        m1.status_code = 200
        m1.json.return_value = r1
        m2 = Mock()
        m2.status_code = 200
        m2.json.return_value = r2
        get_mock.side_effect = [m1, m2]
        containers = [e for e in self.client.get_containers()]
        self.assertEqual(containers[0].dat, 'test_dat_1')
        self.assertEqual(containers[0].uri,
            '/geo/1/containers/test_container_1')
        self.assertEqual(containers[0].id, 'test_container_1')
        self.assertEqual(containers[0].ts_created, 1)
        self.assertEqual(containers[0].ts_modified, 2)
        self.assertEqual(containers[1].dat, 'test_dat_2')
        self.assertEqual(containers[1].uri,
            '/geo/1/containers/test_container_2')
        self.assertEqual(containers[1].id, 'test_container_2')
        self.assertEqual(containers[1].ts_created, 3)
        self.assertEqual(containers[1].ts_modified, 4)
        self.assertEqual(get_mock.call_args_list,
            [call('%s/geo/1/containers' % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_id'},
                  data={},
                  params={},
                  timeout=10,
                  verify=False),
             call('%s/geo/1/containers?page=1&page_size=2' % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_id'},
                  data={},
                  params={},
                  timeout=10,
                  verify=False)])


    @patch.object(requests, 'get')
    def test_get_containers_requests_get_error(self, get_mock):
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
            r = self.client.get_containers())

    @patch.object(requests, 'get')
    def test_get_containers_status_400(self, get_mock):
        get_mock.side_effect = Exception('error')
        self.assertRaises(snowfloat.errors.RequestError,
            r = self.client.get_containers())

    @patch.object(requests, 'put')
    def test_add_containers(self, put_mock):
        r1 = [{'dat': 'test_dat_1',
               'uri': '/geo/1/containers/test_container_1',
               'id': 'test_container_1',
               'ts_created': 1,
               'ts_modified': 2
              },
              {'dat': 'test_dat_2',
               'uri': '/geo/1/containers/test_container_2',
               'id': 'test_container_2',
               'ts_created': 3,
               'ts_modified': 4
              }]
        r2 = [{'dat': 'test_dat_3',
               'uri': '/geo/1/containers/test_container_3',
               'id': 'test_container_3',
               'ts_created': 5,
               'ts_modified': 6
              }]
        m1 = Mock()
        m1.status_code = 200
        m1.json.return_value = r1
        m2 = Mock()
        m2.status_code = 200
        m2.json.return_value = r2
        put_mock.side_effect = [m1, m2]
        containers = [snowfloat.container.Container(dat='test_dat_1'),
                      snowfloat.container.Container(dat='test_dat_2'),
                      snowfloat.container.Container(dat='test_dat_3')]
        containers = self.client.add_containers(containers)
        self.assertEqual(containers[0].dat, 'test_dat_1')
        self.assertEqual(containers[0].uri,
            '/geo/1/containers/test_container_1')
        self.assertEqual(containers[0].id, 'test_container_1')
        self.assertEqual(containers[0].ts_created, 1)
        self.assertEqual(containers[0].ts_modified, 2)
        self.assertEqual(containers[1].dat, 'test_dat_2')
        self.assertEqual(containers[1].uri,
            '/geo/1/containers/test_container_2')
        self.assertEqual(containers[1].id, 'test_container_2')
        self.assertEqual(containers[1].ts_created, 3)
        self.assertEqual(containers[1].ts_modified, 4)
        self.assertEqual(containers[2].dat, 'test_dat_3')
        self.assertEqual(containers[2].uri,
            '/geo/1/containers/test_container_3')
        self.assertEqual(containers[2].id, 'test_container_3')
        self.assertEqual(containers[2].ts_created, 5)
        self.assertEqual(containers[2].ts_modified, 6)
        self.assertEqual(put_mock.call_args_list,
            [call('%s/geo/1/containers' % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_id'},
                  data=json.dumps([{'dat': 'test_dat_1'},
                                   {'dat': 'test_dat_2'}]),
                  params={},
                  timeout=10,
                  verify=False),
             call('%s/geo/1/containers' % (self.url_prefix,),
                  headers={'X-Session-ID': 'test_session_id'},
                  data=json.dumps([{'dat': 'test_dat_3'}]),
                  params={},
                  timeout=10,
                  verify=False)])

    @patch.object(requests, 'delete')
    def test_delete_containers(self, delete_mock):
        m = Mock()
        m.status_code = 200
        m.json.return_value = {}
        delete_mock.return_value = m
        self.client.delete_containers()
        delete_mock.assert_called_with(
            '%s/geo/1/containers' % (self.url_prefix),
            headers={'X-Session-ID': 'test_session_id'},
            data={},
            params={},
            timeout=10,
            verify=False)

    @patch.object(requests, 'get')
    def test_get_geometries(self, get_mock):
        point = snowfloat.geometry.Point(coordinates=[1, 2, 3])
        self.get_geometries_helper(get_mock, self.client.get_geometries,
            'test_container_1', ts_range=(1, 10), query='distance_lte',
            geometry=point, distance=4)

    @patch.object(requests, 'put')
    def test_add_geometries(self, put_mock):
        self.add_geometries_helper(put_mock, self.client.add_geometries,
            'test_container_1', self.geometries)

    @patch.object(requests, 'delete')
    def test_delete_geometries(self, delete_mock):
        self.delete_geometries_helper(delete_mock,
            self.client.delete_geometries,
            'test_container_1', ts_range=(1, 10))

    @patch.object(requests, 'post')
    def test_login(self, post_mock):
        r = {'more': 'test_session_2'}
        m = Mock()
        m.status_code = 200
        m.json.return_value = r
        post_mock.return_value = m
        self.client.login('test_key')
        self.assertEqual(snowfloat.auth.session_id, 'test_session_2')

    @patch.object(requests, 'put')
    def test_add_tasks(self, put_mock):
        r1 = [{'operation': 'test_operation_1',
               'resource': 'test_resource_1',
               'filter': 'test_filter_1',
               'uri': '/geo/1/tasks/test_task_1',
               'id': 'test_task_1',
               'state': 'started',
               'extras': {'extra': 'test_extra_1'},
               'reason': 'test_reason_1',
               'ts_created': 1,
               'ts_modified': 2
              },
              {'operation': 'test_operation_2',
               'resource': 'test_resource_2',
               'filter': 'test_filter_2',
               'uri': '/geo/1/tasks/test_task_2',
               'id': 'test_task_2',
               'state': 'started',
               'extras': {'extra': 'test_extra_2'},
               'reason': 'test_reason_2',
               'ts_created': 3,
               'ts_modified': 4
              }]
        r2 = [{'operation': 'test_operation_3',
               'resource': 'test_resource_3',
               'filter': 'test_filter_3',
               'uri': '/geo/1/tasks/test_task_3',
               'id': 'test_task_3',
               'state': 'started',
               'extras': {'extra': 'test_extra_3'},
               'reason': 'test_reason_3',
               'ts_created': 5,
               'ts_modified': 6
              }]
        m1 = Mock()
        m1.status_code = 200
        m1.json.return_value = r1
        m2 = Mock()
        m2.status_code = 200
        m2.json.return_value = r2
        put_mock.side_effect = [m1, m2]
        tasks = [{'operation': 'test_operation_1',
                  'resource': 'test_resource_1',
                  'filter': 'test_filter_1',
                 },
                 {'operation': 'test_operation_2',
                  'resource': 'test_resource_2',
                  'filter': 'test_filter_2',
                 },
                 {'operation': 'test_operation_3',
                  'resource': 'test_resource_3',
                  'filter': 'test_filter_3',
                 }]
        tasks = self.client._add_tasks(tasks)
        self.assertEqual(tasks[0].operation, 'test_operation_1')
        self.assertEqual(tasks[0].resource, 'test_resource_1')
        self.assertEqual(tasks[0].filter, 'test_filter_1')
        self.assertEqual(tasks[0].id, 'test_task_1')
        self.assertEqual(tasks[0].state, 'started')
        self.assertDictEqual(tasks[0].extras, {'extra': 'test_extra_1'})
        self.assertEqual(tasks[0].reason, 'test_reason_1')
        self.assertEqual(tasks[0].uri, '/geo/1/tasks/test_task_1')
        self.assertEqual(tasks[0].ts_created, 1)
        self.assertEqual(tasks[0].ts_modified, 2)
        self.assertEqual(tasks[1].operation, 'test_operation_2')
        self.assertEqual(tasks[1].resource, 'test_resource_2')
        self.assertEqual(tasks[1].filter, 'test_filter_2')
        self.assertEqual(tasks[1].id, 'test_task_2')
        self.assertEqual(tasks[1].state, 'started')
        self.assertDictEqual(tasks[1].extras, {'extra': 'test_extra_2'})
        self.assertEqual(tasks[1].reason, 'test_reason_2')
        self.assertEqual(tasks[1].uri, '/geo/1/tasks/test_task_2')
        self.assertEqual(tasks[1].ts_created, 3)
        self.assertEqual(tasks[1].ts_modified, 4)
        self.assertEqual(tasks[2].operation, 'test_operation_3')
        self.assertEqual(tasks[2].resource, 'test_resource_3')
        self.assertEqual(tasks[2].filter, 'test_filter_3')
        self.assertEqual(tasks[2].id, 'test_task_3')
        self.assertEqual(tasks[2].state, 'started')
        self.assertDictEqual(tasks[2].extras, {'extra': 'test_extra_3'})
        self.assertEqual(tasks[2].reason, 'test_reason_3')
        self.assertEqual(tasks[2].uri, '/geo/1/tasks/test_task_3')
        self.assertEqual(tasks[2].ts_created, 5)
        self.assertEqual(tasks[2].ts_modified, 6)

    @patch.object(requests, 'get')
    def test_get_task(self, get_mock):
        r = {'operation': 'test_operation_1',
             'resource': 'test_resource_1',
             'filter': 'test_filter_1',
             'uri': '/geo/1/tasks/test_task_1',
             'id': 'test_task_1',
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
        self.assertEqual(task.resource, 'test_resource_1')
        self.assertEqual(task.filter, 'test_filter_1')
        self.assertEqual(task.uri, '/geo/1/tasks/test_task_1')
        self.assertEqual(task.id, 'test_task_1')
        self.assertEqual(task.state, 'started')
        self.assertDictEqual(task.extras, {'extra': 'test_extra_1'})
        self.assertEqual(task.reason, 'test_reason_1')
        self.assertEqual(task.ts_created, 1)
        self.assertEqual(task.ts_modified, 2)
        get_mock.assert_called_with(
            '%s/geo/1/tasks/test_task_1' % (self.url_prefix,),
            headers={'X-Session-ID': 'test_session_id'},
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
        task1.id = 'test_task_1'
        task2 = Mock()
        task2.id = 'test_task_2'
        _add_tasks_mock.return_value = [task1, task2]
        t1 = Mock()
        t1.state = 'success'
        t2 = Mock()
        t2.state = 'success'
        _get_task_mock.side_effect = [t1, t2]
        result1 = Mock()
        result1.dat = json.dumps('test_result_1')
        result2 = Mock()
        result2.dat = json.dumps('test_result_2')
        _get_results_mock.side_effect = [[result1], [result2]]
        tasks = [
            {'operation': 'test_operation_1',
             'resource': 'points',
             'container_id': 'test_container_1',
             'ts_range': (0, 10)},
            {'operation': 'test_operation_2',
             'resource': 'points',
             'container_id': 'test_container_2',
             'ts_range': (1, 11)},
        ]
        r = self.client.execute_tasks(tasks)
        self.assertListEqual(r, [['test_result_1',], ['test_result_2',]])
        d = [
            {'operation': 'test_operation_1',
             'resource': 'points',
             'type__exact': 'Point',
             'ts__gte': 0,
             'ts__lte': 10,
             'container__sid': 'test_container_1'},
            {'operation': 'test_operation_2',
             'resource': 'points',
             'type__exact': 'Point',
             'ts__gte': 1,
             'ts__lte': 11,
             'container__sid': 'test_container_2'}
        ]
        _add_tasks_mock.assert_called_with(d)
        self.assertEqual(_get_task_mock.call_args_list,
            [call(task1.id), call(task2.id)])
        self.assertEqual(_get_results_mock.call_args_list,
            [call(task1.id), call(task2.id)])

    @patch.object(snowfloat.client.Client, '_get_results')
    @patch.object(snowfloat.client.Client, '_get_task')
    @patch.object(snowfloat.client.Client, '_add_tasks')
    def test_execute_tasks_task_failure(self, _add_tasks_mock, _get_task_mock,
            _get_results_mock):
        task1 = Mock()
        task1.id = 'test_task_1'
        task2 = Mock()
        task2.id = 'test_task_2'
        _add_tasks_mock.return_value = [task1, task2]
        t1 = Mock()
        t1.state = 'success'
        t2 = Mock()
        t2.state = 'failure'
        t2.reason = 'test_reason'
        _get_task_mock.side_effect = [t1, t2]
        result1 = Mock()
        result1.dat = json.dumps('test_result_1')
        result2 = Mock()
        result2.dat = json.dumps('test_result_2')
        _get_results_mock.side_effect = [[result1], [result2]]
        tasks = [
            {'operation': 'test_operation_1',
             'resource': 'points',
             'container_ids': ['test_container_1', 'test_container_1b'],
             'ts_range': (0, 10)},
            {'operation': 'test_operation_2',
             'resource': 'points',
             'container_id': 'test_container_2',
             'ts_range': (1, 11)},
        ]
        r = self.client.execute_tasks(tasks)
        self.assertListEqual(r, [['test_result_1',], {'error': 'test_reason'}])
        d = [
            {'operation': 'test_operation_1',
             'resource': 'points',
             'type__exact': 'Point',
             'ts__gte': 0,
             'ts__lte': 10,
             'container__sid__in': ['test_container_1', 'test_container_1b']},
            {'operation': 'test_operation_2',
             'resource': 'points',
             'type__exact': 'Point',
             'ts__gte': 1,
             'ts__lte': 11,
             'container__sid': 'test_container_2'}
        ]
        _add_tasks_mock.assert_called_with(d)
        self.assertEqual(_get_task_mock.call_args_list,
            [call(task1.id), call(task2.id)])
        self.assertEqual(_get_results_mock.call_args_list,
            [call(task1.id),])

    @patch.object(snowfloat.client.Client, '_get_results')
    @patch.object(snowfloat.client.Client, '_get_task')
    @patch.object(snowfloat.client.Client, '_add_tasks')
    def test_execute_tasks_task_wait(self, _add_tasks_mock, _get_task_mock,
            _get_results_mock):
        task1 = Mock()
        task1.id = 'test_task_1'
        task2 = Mock()
        task2.id = 'test_task_2'
        _add_tasks_mock.return_value = [task1, task2]
        t1 = Mock()
        t1.state = 'success'
        t2 = Mock()
        t2.state = 'started'
        t3 = Mock()
        t3.state = 'success'
        _get_task_mock.side_effect = [t1, t2, t3]
        result1 = Mock()
        result1.dat = json.dumps('test_result_1')
        result2 = Mock()
        result2.dat = json.dumps('test_result_2')
        _get_results_mock.side_effect = [[result1], [result2]]
        tasks = [
            {'operation': 'test_operation_1',
             'resource': 'points',
             'container_id': 'test_container_1',
             'ts_range': (0, 10)},
            {'operation': 'test_operation_2',
             'resource': 'points',
             'container_id': 'test_container_2',
             'ts_range': (1, 11)},
        ]
        r = self.client.execute_tasks(tasks, interval=0.1)
        self.assertListEqual(r, [['test_result_1',], ['test_result_2',]])
        d = [
            {'operation': 'test_operation_1',
             'resource': 'points',
             'type__exact': 'Point',
             'ts__gte': 0,
             'ts__lte': 10,
             'container__sid': 'test_container_1'},
            {'operation': 'test_operation_2',
             'resource': 'points',
             'type__exact': 'Point',
             'ts__gte': 1,
             'ts__lte': 11,
             'container__sid': 'test_container_2'}
        ]
        _add_tasks_mock.assert_called_with(d)
        self.assertEqual(_get_task_mock.call_args_list,
            [call(task1.id), call(task2.id), call(task2.id)])
        self.assertEqual(_get_results_mock.call_args_list,
            [call(task1.id), call(task2.id)])

    @patch.object(snowfloat.client.Client, '_get_results')
    @patch.object(snowfloat.client.Client, '_get_task')
    @patch.object(snowfloat.client.Client, '_add_tasks')
    def test_execute_tasks_task_request_error(self, _add_tasks_mock, _get_task_mock,
            _get_results_mock):
        task1 = Mock()
        task1.id = 'test_task_1'
        task2 = Mock()
        task2.id = 'test_task_2'
        _add_tasks_mock.return_value = [task1, task2]
        t1 = Mock()
        t1.state = 'success'
        _get_task_mock.side_effect = [t1,
            snowfloat.errors.RequestError(500, 0, '', '')]
        result1 = Mock()
        result1.dat = json.dumps('test_result_1')
        result2 = Mock()
        result2.dat = json.dumps('test_result_2')
        _get_results_mock.side_effect = [[result1], [result2]]
        tasks = [
            {'operation': 'test_operation_1',
             'resource': 'points',
             'container_id': 'test_container_1',
             'ts_range': (0, 10)},
            {'operation': 'test_operation_2',
             'resource': 'points',
             'container_id': 'test_container_2',
             'ts_range': (1, 11)},
        ]
        r = self.client.execute_tasks(tasks)
        self.assertListEqual(r, [['test_result_1',], None])
        d = [
            {'operation': 'test_operation_1',
             'resource': 'points',
             'type__exact': 'Point',
             'ts__gte': 0,
             'ts__lte': 10,
             'container__sid': 'test_container_1'},
            {'operation': 'test_operation_2',
             'resource': 'points',
             'type__exact': 'Point',
             'ts__gte': 1,
             'ts__lte': 11,
             'container__sid': 'test_container_2'}
        ]
        _add_tasks_mock.assert_called_with(d)
        self.assertEqual(_get_task_mock.call_args_list,
            [call(task1.id), call(task2.id)])
        self.assertEqual(_get_results_mock.call_args_list,
            [call(task1.id),])


class ContainerTests(Tests):
   
    container = snowfloat.container.Container('test_dat_1', 'test_container_1',
        1, 2, '/geo/1/containers/test_container_1')

    @patch.object(requests, 'get')
    def test_get_geometries(self, get_mock):
        point = snowfloat.geometry.Point(coordinates=(1, 2, 3))
        self.get_geometries_helper(get_mock, self.container.get_geometries,
            ts_range=(1, 10), query='distance_lte',
            geometry=point, distance=4)
    
    @patch.object(requests, 'put')
    def test_add_geometries(self, put_mock):
        self.add_geometries_helper(put_mock, self.container.add_geometries,
            self.geometries)

    @patch.object(requests, 'delete')
    def test_delete_geometries(self, delete_mock):
        self.delete_geometries_helper(delete_mock,
            self.container.delete_geometries, ts_range=(1, 10))

    @patch.object(requests, 'post')
    def test_update(self, post_mock):
        m = Mock()
        m.status_code = 200
        m.json.return_value = {}
        post_mock.return_value = m
        self.container.update(dat='test_dat')
        post_mock.assert_called_with(
            '%s/geo/1/containers/test_container_1' % (self.url_prefix),
            headers={'X-Session-ID': 'test_session_id'},
            data=json.dumps({'dat': 'test_dat'}),
            params={},
            timeout=10,
            verify=False)
        self.assertEqual(self.container.dat, 'test_dat')

    @patch.object(requests, 'delete')
    def test_delete(self, delete_mock):
        m = Mock()
        m.status_code = 200
        m.json.return_value = {}
        delete_mock.return_value = m
        self.container.delete()
        delete_mock.assert_called_with(
            '%s/geo/1/containers/test_container_1' % (self.url_prefix),
            headers={'X-Session-ID': 'test_session_id'},
            data={},
            params={},
            timeout=10,
            verify=False)


class ResultsTests(Tests):
   
    task = snowfloat.task.Task('test_task_1',
        '/geo/1/tasks/test_task_1',
        'test_operation_1',
        'test_resource_1',
        'test_filter_1',
        'started',
        '{}',
        '',
        1, 2)

    @patch.object(requests, 'get')
    def test_get_results(self, get_mock):
        self.get_results_helper(get_mock, self.task._get_results)


class GeometriesTests(Tests):
        
    point = snowfloat.geometry.Point(coordinates=[1, 2, 3],
        uri='/geo/1/containers/test_container_1/geometries/test_geometry_1',
        ts=1, dat='test_dat')

    @patch.object(requests, 'post')
    def test_update(self, post_mock):
        m = Mock()
        m.status_code = 200
        m.json.return_value = {}
        post_mock.return_value = m
        self.point.update(coordinates=[4, 5, 6], ts=2, dat='test_dat_1')
        d = {'type': 'Feature',
             'geometry': {'type': 'Point',
                          'coordinates': [4, 5, 6]},
             'properties': {
                  'ts': 2,
                  'dat': 'test_dat_1',
              }}
        post_mock.assert_called_with(
            '%s/geo/1/containers/test_container_1/'\
                'geometries/test_geometry_1' % (self.url_prefix),
            headers={'X-Session-ID': 'test_session_id'},
            data=json.dumps(d),
            params={},
            timeout=10,
            verify=False)
        self.assertListEqual(self.point.coordinates, [4, 5, 6])
        self.assertEqual(self.point.ts, 2)
        self.assertEqual(self.point.dat, 'test_dat_1')

    @patch.object(requests, 'delete')
    def test_delete(self, delete_mock):
        self.delete_geometry_helper(delete_mock,
            self.point.delete)


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


if __name__ == "__main__":
    unittest.main()
