"""Task results."""

class Result(object):
    """Task result.

    Attributes:
        uuid (str): UUID.

        uri (str): URI.

        tag: Tag data.

        ts_created: Creation timestamp.

        ts_modified: Modification timestamp.
    """
    uuid = None
    uri = None
    tag = None
    ts_created = None
    ts_modified = None

    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)

def parse_results(results):
    """Convert results dictionaries to Result objects.

    Args:
        results (list): List of result dictionaries.

    Returns:
        list: List of Result objects.
    """
    return [Result(uuid=r['uuid'],
                   uri=r['uri'],
                   tag=r['tag'],
                   ts_created=r['ts_created'],
                   ts_modified=r['ts_modified']) for r in results]
