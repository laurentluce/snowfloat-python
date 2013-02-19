import time

import snowfloat.request
import snowfloat.result

class Operations(object):
    stats = 1
    map = 2

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

    def __init__(self, id, uri, operation, resource, filter, state, extras,
            reason, ts_created, ts_modified):
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

    def _get_results(self):
        uri = '%s/results' % (self.uri)
        data = {}
        for r in snowfloat.request.get(uri, data):
            # convert list of json results to Result objects
            results = snowfloat.result.parse_results(r['results'])
            for r in results:
                yield r


def parse_tasks(tasks):
    return [Task(t['id'],
                 t['uri'],
                 t['operation'],
                 t['resource'],
                 t['filter'],
                 t['state'],
                 t['extras'],
                 t['reason'],
                 t['ts_created'],
                 t['ts_modified']) for t in tasks]
