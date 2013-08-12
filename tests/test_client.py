"""Client tests."""
import json
import os
import tempfile

from mock import Mock, patch, call
import requests
import requests.exceptions

import tests.helper

import snowfloat.client
import snowfloat.errors
import snowfloat.geometry
import snowfloat.task

class ClientTests(tests.helper.Tests):
    """Client tests."""
   
    @patch.object(requests, 'get')
    def test_get_layers(self, get_mock):
        """Get layers."""
        get_mock.__name__ = 'get'
        res_1 = {
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
                            'srid': 4326,
                            'dims': 3,
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
                            'srid': 4327,
                            'dims': 2,
                            'extent': None
                           }],
            }
        res_2 = {
                'next_page_uri': None,
                'total': 0,
                'layers': [],
            }
        mock_1 = Mock()
        mock_1.status_code = 200
        mock_1.json.return_value = res_1
        mock_2 = Mock()
        mock_2.status_code = 200
        mock_2.json.return_value = res_2
        get_mock.side_effect = [mock_1, mock_2]
        layers = self.client.get_layers(
            name_exact='test_name',
            order_by=('-name', 'date_created'),
            query_slice=(1, 20))
        
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
        self.assertEqual(layers[0].srid, 4326)
        self.assertEqual(layers[0].dims, 3)
        self.assertListEqual(layers[0].extent, [1, 2, 3, 4])
        self.assertEqual(str(layers[0]),
            'Layer(name=test_tag_1, uuid=test_layer_1, date_created=1, '\
            'date_modified=2, uri=/geo/1/layers/test_layer_1, '\
            'num_features=10, num_points=20, '\
            'fields=[{\'type\': \'string\', \'name\': \'field_1\', '\
            '\'size\': 256}], srid=4326, dims=3, '\
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
        self.assertEqual(layers[1].srid, 4327)
        self.assertEqual(layers[1].dims, 2)
        self.assertIsNone(layers[1].extent)
        self.method_mock_assert_call_args_list(get_mock,
            '/geo/1/layers',
            '/geo/1/layers?page=1&page_size=2',
            params={'name__exact': 'test_name',
                    'order_by': '-name,date_created',
                    'slice_start': 1,
                    'slice_end': 20})

    @patch.object(requests, 'get')
    def test_get_layers_status_code_413(self, get_mock):
        """Get layers returning 413."""
        get_mock.__name__ = 'get'
        mock = Mock()
        mock.status_code = 413
        mock.json.return_value = {'code': 1, 'message': 'test_message',
            'more': 'test_more'}
        get_mock.return_value = mock
        self.assertRaises(snowfloat.errors.RequestError,
            self.client.get_layers)

    @patch.object(requests, 'get')
    def test_get_layers_get_error(self, get_mock):
        """Get layers timing out."""
        get_mock.__name__ = 'get'
        get_mock.side_effect = requests.exceptions.RequestException('timeout')
        self.assertRaises(snowfloat.errors.RequestError,
            self.client.get_layers)

    @patch.object(requests, 'post')
    def test_add_layers(self, post_mock):
        """Add layers."""
        post_mock.__name__ = 'post'
        res = [{'name': 'test_tag_1',
              'uri': '/geo/1/layers/test_layer_1',
              'uuid': 'test_layer_1',
              'date_created': 1,
              'date_modified': 2,
              'num_features': 0,
              'num_points': 0,
              'fields': [{'name': 'field_1', 'type': 'string', 'size': 256},],
              'srid': 4326,
              'dims': 3,
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
              'srid': 4327,
              'dims': 2,
              'extent': None
             }]
        mock = Mock()
        mock.status_code = 200
        mock.json.return_value = res
        post_mock.return_value = mock
        layers = [
            snowfloat.layer.Layer(name='test_tag_1',
                fields=[{'name': 'field_1', 'type': 'string', 'size': 256},],
                srid=4326, dims=3,
                extent=[1, 2, 3, 4]),
            snowfloat.layer.Layer(name='test_tag_2',
                fields=[{'name': 'field_2', 'type': 'string', 'size': 256},],
                srid=4327, dims=2),
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
        self.assertEqual(layers[0].srid, 4326)
        self.assertEqual(layers[0].dims, 3)
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
        self.assertEqual(layers[1].srid, 4327)
        self.assertEqual(layers[1].dims, 2)
        self.assertIsNone(layers[1].extent)
        
        self.assertEqual(post_mock.call_args_list,
            [call('%s/geo/1/layers' % (tests.helper.URL_PREFIX,),
                  headers=tests.helper.get_request_body_headers(),
                  data=json.dumps([
                    {'name': 'test_tag_1',
                     'fields': [{'name': 'field_1', 'type': 'string',
                                 'size': 256},],
                     'srid': 4326,
                     'dims': 3,
                     'extent': [1, 2, 3, 4]},
                    {'name': 'test_tag_2',
                     'fields': [{'name': 'field_2', 'type': 'string',
                                 'size': 256},],
                     'srid': 4327,
                     'dims': 2}
                    ]),
                  params={},
                  timeout=10,
                  verify=False)])

    @patch.object(requests, 'delete')
    def test_delete_layers(self, delete_mock):
        """Delete layers."""
        tests.helper.set_method_mock(delete_mock, 'delete', 200, {})
        self.client.delete_layers()
        tests.helper.method_mock_assert_called_with(delete_mock,
            '/geo/1/layers') 

    @patch.object(requests, 'get')
    def test_get_features(self, get_mock):
        """Get layer features."""
        self.get_features_test(get_mock, self.client.get_features,
            'test_layer_1')

    @patch.object(requests, 'post')
    def test_add_features(self, post_mock):
        """Add layer features."""
        post_mock.__name__ = 'post'
        self.add_features_helper(post_mock, self.client.add_features,
            'test_layer_1', self.features)

    @patch.object(requests, 'delete')
    def test_delete_features(self, delete_mock):
        """Delete layer features."""
        delete_mock.__name__ = 'delete'
        self.delete_features_helper(delete_mock,
            self.client.delete_features,
            'test_layer_1', field_ts_gte=1, field_ts_lte=10,
            date_created_lte='2002-12-25 00:00:00-00:00')

    @patch.object(requests, 'post')
    def test_add_tasks(self, post_mock):
        """Add tasks."""
        post_mock.__name__ = 'post'
        res = [{'operation': 'test_operation_1',
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
        mock = Mock()
        mock.status_code = 200
        mock.json.return_value = res
        post_mock.return_value = mock
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
            'spatial={\'spatial_1\': \'test_task_spatial_1\'}, state=started, '\
            'extras={\'extra\': \'test_extra_1\'}, reason=test_reason_1)')
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
        """Get task."""
        get_mock.__name__ = 'get'
        res = {'operation': 'test_operation_1',
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
        mock = Mock()
        mock.status_code = 200
        mock.json.return_value = res
        get_mock.return_value = mock
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
        tests.helper.method_mock_assert_called_with(get_mock,
            '/geo/1/tasks/test_task_1')

    @patch.object(requests, 'get')
    def test_get_results(self, get_mock):
        """Get task results."""
        get_mock.__name__ = 'get'
        self.get_results_helper(get_mock, self.client._get_results,
            'test_task_1')

    @patch.object(snowfloat.client.Client, '_get_results')
    @patch.object(snowfloat.client.Client, '_get_task')
    @patch.object(snowfloat.client.Client, '_add_tasks')
    def test_execute_tasks(self, _add_tasks_mock, _get_task_mock,
        _get_results_mock):
        """Execute asynchronous tasks."""
        task_1 = Mock()
        task_1.uuid = 'test_task_1'
        task_2 = Mock()
        task_2.uuid = 'test_task_2'
        _add_tasks_mock.return_value = [task_1, task_2]
        task_3 = Mock()
        task_3.state = 'success'
        task_4 = Mock()
        task_4.state = 'success'
        _get_task_mock.side_effect = [task_3, task_4]
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
        res = self.client.execute_tasks(tasks)
        self.assertListEqual(res, [['test_result_1',], ['test_result_2',]])
        self.execute_tasks_add_tasks_helper(_add_tasks_mock)

    # pylint: disable=R0201
    def execute_tasks_add_tasks_helper(self, add_tasks_mock):
        """Execute tasks add helper."""
        tasks = [
            {'operation': 'test_operation_1',
             'filter': {'layer__uuid__exact': 'test_layer_1'},
             'spatial': {},
             'extras': {}},
            {'operation': 'test_operation_2',
             'filter': {'layer__uuid__exact': 'test_layer_2'},
             'spatial': {},
             'extras': {}},
        ]
        add_tasks_mock.assert_called_with(tasks)

    @patch.object(snowfloat.client.Client, '_get_results')
    @patch.object(snowfloat.client.Client, '_get_task')
    @patch.object(snowfloat.client.Client, '_add_tasks')
    def test_execute_tasks_task_failure(self, _add_tasks_mock, _get_task_mock,
            _get_results_mock):
        """Execute tasks with a task failure."""
        task_1 = Mock()
        task_1.uuid = 'test_task_1'
        task_2 = Mock()
        task_2.uuid = 'test_task_2'
        _add_tasks_mock.return_value = [task_1, task_2]
        task_3 = Mock()
        task_3.state = 'success'
        task_4 = Mock()
        task_4.state = 'failure'
        task_4.reason = 'test_reason'
        _get_task_mock.side_effect = [task_3, task_4]
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
        res = self.client.execute_tasks(tasks)
        self.assertListEqual(res, [['test_result_1',],
            {'error': 'test_reason'}])
        self.execute_tasks_add_tasks_helper(_add_tasks_mock)
        self.assertEqual(_get_task_mock.call_args_list,
            [call(task_1.uuid), call(task_2.uuid)])
        self.assertEqual(_get_results_mock.call_args_list,
            [call(task_1.uuid),])

    @patch.object(snowfloat.client.Client, '_get_results')
    @patch.object(snowfloat.client.Client, '_get_task')
    @patch.object(snowfloat.client.Client, '_add_tasks')
    def test_execute_tasks_task_wait(self, _add_tasks_mock, _get_task_mock,
            _get_results_mock):
        """Execute tasks waiting for a task to finish."""
        task_1 = Mock()
        task_1.uuid = 'test_task_1'
        task_2 = Mock()
        task_2.uuid = 'test_task_2'
        _add_tasks_mock.return_value = [task_1, task_2]
        task_3 = Mock()
        task_3.state = 'success'
        task_4 = Mock()
        task_4.state = 'started'
        task_5 = Mock()
        task_5.state = 'success'
        _get_task_mock.side_effect = [task_3, task_4, task_5]
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
        res = self.client.execute_tasks(tasks, interval=0.1)
        self.assertListEqual(res, [['test_result_1',], ['test_result_2',]])
        self.execute_tasks_add_tasks_helper(_add_tasks_mock)
        self.assertEqual(_get_task_mock.call_args_list,
            [call(task_1.uuid), call(task_2.uuid), call(task_2.uuid)])
        self.assertEqual(_get_results_mock.call_args_list,
            [call(task_1.uuid), call(task_2.uuid)])

    @patch.object(snowfloat.client.Client, '_get_results')
    @patch.object(snowfloat.client.Client, '_get_task')
    @patch.object(snowfloat.client.Client, '_add_tasks')
    # pylint: disable=C0103 
    def test_execute_tasks_task_request_error(self, _add_tasks_mock,
            _get_task_mock, _get_results_mock):
        """Execute tasks with request failing."""
        task_1 = Mock()
        task_1.uuid = 'test_task_1'
        task_2 = Mock()
        task_2.uuid = 'test_task_2'
        _add_tasks_mock.return_value = [task_1, task_2]
        task_3 = Mock()
        task_3.state = 'success'
        _get_task_mock.side_effect = [task_3,
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
        res = self.client.execute_tasks(tasks)
        self.assertListEqual(res, [['test_result_1',], None])
        self.execute_tasks_add_tasks_helper(_add_tasks_mock)
        self.assertEqual(_get_task_mock.call_args_list,
            [call(task_1.uuid), call(task_2.uuid)])
        self.assertEqual(_get_results_mock.call_args_list,
            [call(task_1.uuid),])


class ImportDataSourceTests(tests.helper.Tests):
    """Import data source tests."""
    @patch.object(requests, 'get')
    @patch.object(requests, 'delete')
    @patch.object(snowfloat.client.Client, 'execute_tasks')
    @patch.object(requests, 'post')
    def test_import_geospatial_data(self, post_mock, execute_tasks_mock,
            delete_mock, get_mock):
        """Import data source test."""
        get_mock.__name__ = 'get'
        delete_mock.__name__ = 'delete'
        post_mock.__name__ = 'post'
        res = {'uuid': 'test_blob_uuid'}
        mock_1 = Mock()
        mock_1.status_code = 200
        mock_1.json.return_value = res
        post_mock.return_value = mock_1
        mock_2 = Mock()
        mock_2.status_code = 200
        mock_2.json.side_effect = snowfloat.errors.RequestError(status=500,
            code=None, message=None, more=None)
        delete_mock.return_value = mock_2
        mock_3 = Mock()
        mock_3.status_code = 200
        mock_3.json.side_effect = [
            {'uuid': 'test_blob_uuid', 'state': 'started'},
            {'uuid': 'test_blob_uuid', 'state': 'success'}]
        get_mock.return_value = mock_3
        execute_tasks_mock.return_value = [['test_result',]]
        tfile = tempfile.NamedTemporaryFile(delete=False)
        tfile.close()
        res = self.client.import_geospatial_data(tfile.name, srid=4326,
            state_check_interval=0.1)
        self.assertEqual(res, 'test_result')
        self.import_geospatial_data_helper(post_mock, get_mock, delete_mock,
            execute_tasks_mock)
        os.remove(tfile.name)

    @patch.object(requests, 'get')
    @patch.object(requests, 'delete')
    @patch.object(snowfloat.client.Client, 'execute_tasks')
    @patch.object(requests, 'post')
    # pylint: disable=C0103 
    def test_import_geospatial_data_task_error(self, post_mock,
            execute_tasks_mock, delete_mock, get_mock):
        """Import data source test with task failing."""
        get_mock.__name__ = 'get'
        delete_mock.__name__ = 'delete'
        post_mock.__name__ = 'post'
        res = {'uuid': 'test_blob_uuid'}
        mock_1 = Mock()
        mock_1.status_code = 200
        mock_1.json.return_value = res
        post_mock.return_value = mock_1
        mock_2 = Mock()
        mock_2.status_code = 200
        mock_2.json.side_effect = snowfloat.errors.RequestError(status=500,
            code=None, message=None, more=None)
        delete_mock.return_value = mock_2
        mock_3 = Mock()
        mock_3.status_code = 200
        mock_3.json.side_effect = [
            {'uuid': 'test_blob_uuid', 'state': 'started'},
            {'uuid': 'test_blob_uuid', 'state': 'success'}]
        get_mock.return_value = mock_3
        execute_tasks_mock.return_value = [{'error': 'test_error'},]
        tfile = tempfile.NamedTemporaryFile(delete=False)
        tfile.close()
        self.assertRaises(snowfloat.errors.RequestError,
            self.client.import_geospatial_data, tfile.name, srid=4326,
            state_check_interval=0.1)
        self.import_geospatial_data_helper(post_mock, get_mock, delete_mock,
            execute_tasks_mock)
        os.remove(tfile.name)

    @patch.object(requests, 'get')
    @patch.object(requests, 'delete')
    @patch.object(snowfloat.client.Client, 'execute_tasks')
    @patch.object(requests, 'post')
    # pylint: disable=C0103 
    def test_import_geospatial_data_blob_state_failure(self, post_mock,
            execute_tasks_mock, delete_mock, get_mock):
        """Import data source test with upload blob failing."""
        get_mock.__name__ = 'get'
        delete_mock.__name__ = 'delete'
        post_mock.__name__ = 'post'
        res = {'uuid': 'test_blob_uuid'}
        mock_1 = Mock()
        mock_1.status_code = 200
        mock_1.json.return_value = res
        post_mock.return_value = mock_1
        mock_2 = Mock()
        mock_2.status_code = 200
        mock_2.json.return_value = {}
        delete_mock.return_value = mock_2
        mock_3 = Mock()
        mock_3.status_code = 200
        mock_3.json.side_effect = [
            {'uuid': 'test_blob_uuid', 'state': 'started'},
            {'uuid': 'test_blob_uuid', 'state': 'failure'}]
        get_mock.return_value = mock_3
        tfile = tempfile.NamedTemporaryFile(delete=False)
        tfile.close()
        self.assertRaises(snowfloat.errors.RequestError,
            self.client.import_geospatial_data, tfile.name,
            state_check_interval=0.1)
        self.import_geospatial_data_helper(post_mock, get_mock, delete_mock,
            execute_tasks_mock, validate_execute_tasks=False)
        os.remove(tfile.name)

    # pylint: disable=R0913
    def import_geospatial_data_helper(self, post_mock, get_mock, delete_mock,
            execute_tasks_mock, validate_execute_tasks=True):
        """Import data source test helper."""
        # validate post call
        call_args = post_mock.call_args
        self.assertEqual(call_args[0][0], '%s/geo/1/blobs' % (
            tests.helper.URL_PREFIX,))
        self.assertDictEqual(call_args[1]['headers'],
            {'Authorization':
                 'GEO IY3487E2J6ZHFOW5A7P5:'\
                 'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I=',
             'Content-Sha':
                 'n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg=',
             'Content-Type': 'application/octet-stream',
             'Date': 'Sat, 08 Jun 2013 22:12:05 GMT'})
        self.assertDictEqual(call_args[1]['params'], {})
        self.assertEqual(call_args[1]['timeout'], 10)
        self.assertEqual(call_args[1]['verify'], False)
        # validate get call
        call_args = get_mock.call_args
        self.assertEqual(call_args[0][0],
            '%s/geo/1/blobs/test_blob_uuid' % (tests.helper.URL_PREFIX,))
        self.assertDictEqual(call_args[1]['headers'],
            {'Authorization':
                 'GEO IY3487E2J6ZHFOW5A7P5:'\
                 'YDA64iuZiGG847KPM+7BvnWKITyGyTwHbb6fVYwRx1I=',
             'Date': 'Sat, 08 Jun 2013 22:12:05 GMT'})
        self.assertDictEqual(call_args[1]['params'], {})
        self.assertEqual(call_args[1]['timeout'], 10)
        self.assertEqual(call_args[1]['verify'], False)
        # validate execute_tasks call
        if validate_execute_tasks:
            call_args = execute_tasks_mock.call_args
            task = call_args[0][0][0]
            self.assertEqual(task.operation, 'import_geospatial_data')
            self.assertDictEqual(task.extras, {'blob_uuid': 'test_blob_uuid',
                                               'srid': 4326})
        else:
            self.assertFalse(execute_tasks_mock.called)
        tests.helper.method_mock_assert_called_with(delete_mock,
            '/geo/1/blobs/test_blob_uuid') 
