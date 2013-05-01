"""Asynchronous tasks."""

import snowfloat.request
import snowfloat.result

class Task(object):
    """Asynchronous task sent to the server.

    Attributes:

        uuid (str): Task UUID.

        uri (str): Task URI.

        operation (str): Task operation: distance, map...

        resource (str): Task geometries resource: points, polygons...

        task_filter (dict): Query filter.

        state (str): Task state: running, succeed...

        extras (dict): Optional task parameters.

        reason (str): Task error reason.

        ts_created (int): Creation timestamp.

        ts_modified (int): Modification timestamp.
    """
    uuid = None
    uri = None
    operation = None
    resource = None
    task_filter = None
    state = None
    extras = None
    reason = None
    ts_created = None
    ts_modified = None
    ts_range = None
    container_uuid = None

    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            getattr(self, key)
            setattr(self, key, val)

    def get_results(self):
        """Returns the task results.

        Returns:
            generator. Yields Result objects.
        """
        uri = '%s/results' % (self.uri)
        data = {}
        for res in snowfloat.request.get(uri, data):
            # convert list of json results to Result objects
            results = snowfloat.result.parse_results(res['results'])
            for result in results:
                yield result

def parse_tasks(tasks):
    """Convert task dictionaries to Task objects.

    Args:
        tasks (list): List of task dictionaries.

    Returns:
        list: List of Task objects.
    """
    return [Task(operation=t['operation'],
                 resource=t['resource'],
                 uuid=t['uuid'],
                 uri=t['uri'],
                 task_filter=t['task_filter'],
                 extras=t['extras'],
                 state=t['state'],
                 reason=t['reason'],
                 ts_created=t['ts_created'],
                 ts_modified=t['ts_modified']) for t in tasks]
