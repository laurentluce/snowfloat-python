import time

class Result(object):

    id = None
    uri = None
    dat = None
    ts_created = None
    ts_modified = None

    def __init__(self, id, uri, dat, ts_created, ts_modified):
        self.id = id
        self.uri = uri
        self.dat = dat
        self.ts_created = ts_created
        self.ts_modified = ts_modified


def parse_results(results):
    return [Result(r['id'],
                   r['uri'],
                   r['dat'],
                   r['ts_created'],
                   r['ts_modified']) for r in results]
