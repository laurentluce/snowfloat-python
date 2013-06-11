"""Asynchronous tasks."""

import snowfloat.request
import snowfloat.result

class Task(object):
    """Asynchronous task sent to the server.

    Attributes:

        uuid (str): Task UUID.

        uri (str): Task URI.

        operation (str): Task operation: map.

        task_filter (dict): Query filter.

        state (str): Task state: running, succeed...

        extras (dict): Optional task parameters.

        reason (str): Task error reason.

        date_created (str): Creation date in ISO format.

        date_modified (str): Modification date in ISO format.
    """
    uuid = None
    uri = None
    operation = None
    layer_uuid = None
    task_filter = None
    state = None
    extras = None
    reason = None
    date_created = None
    date_modified = None

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

    def __str__(self):
        return '%s(uuid=%s, uri=%s, ' \
                'date_created=%s, date_modified=%s, ' \
                'operation=%s, ' \
                'layer_uuid=%s, task_filter=%s ' \
                'state=%s, extras=%s ' \
                'reason=%s' \
            % (self.__class__.__name__,
               self.uuid, self.uri, self.date_created, self.date_modified,
               self.operation,
               self.layer_uuid, self.task_filter,
               self.state, self.extras, self.reason)


def parse_tasks(tasks):
    """Convert task dictionaries to Task objects.

    Args:
        tasks (list): List of task dictionaries.

    Returns:
        list: List of Task objects.
    """
    return [Task(operation=t['operation'],
                 uuid=t['uuid'],
                 uri=t['uri'],
                 task_filter=t['task_filter'],
                 extras=t['extras'],
                 state=t['state'],
                 reason=t['reason'],
                 date_created=t['date_created'],
                 date_modified=t['date_modified']) for t in tasks]
