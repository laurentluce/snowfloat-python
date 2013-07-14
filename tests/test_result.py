"""Task results tests."""
from mock import patch
import requests
import requests.exceptions

import tests.helper

import snowfloat.task

class ResultsTests(tests.helper.Tests):
    """Task results tests."""
   
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
        """Get task results."""
        get_mock.__name__ = 'get'
        self.get_results_helper(get_mock, self.task.get_results)



