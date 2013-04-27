import time

import snowfloat.request
import snowfloat.result

class Task(object):

    id = None
    uri = None
    operation = None
    resource = None
    filter = None
    state = None
    extras = None
    reason = None
    ts_created = None
    ts_modified = None

    def __init__(self, operation, resource, id=None, uri=None, filter=None,
            extras=None, state=None, reason=None,
            ts_created=None, ts_modified=None, container_id=None,
            ts_range=None):
        self.id = id
        self.uri = uri
        self.operation = operation
        self.resource = resource
        self.filter = filter
        self.state = state
        self.extras = extras
        self.reason = reason
        self.ts_created = ts_created
        self.ts_modified = ts_modified
        self.container_id = container_id
        self.ts_range = ts_range

    def _get_results(self):
        uri = '%s/results' % (self.uri)
        data = {}
        for r in snowfloat.request.get(uri, data):
            # convert list of json results to Result objects
            results = snowfloat.result.parse_results(r['results'])
            for r in results:
                yield r

def parse_tasks(tasks):
    return [Task(t['operation'],
                 t['resource'],
                 t['id'],
                 t['uri'],
                 t['filter'],
                 t['extras'],
                 t['state'],
                 t['reason'],
                 t['ts_created'],
                 t['ts_modified']) for t in tasks]
