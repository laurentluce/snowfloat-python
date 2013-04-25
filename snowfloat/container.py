import json
import time

import snowfloat.geometry
import snowfloat.request

class Container(object):

    dat = None
    id = None
    ts_created = None
    ts_modified = None
    uri = None

    def __init__(self, dat='', id=None, ts_created=None, ts_modified=None,
            uri=None):
        self.dat = dat
        self.id = id
        self.ts_created = ts_created
        self.ts_modified = ts_modified
        self.uri = uri
    
    def __str__(self):
        return 'Container(dat=%s, id=%s, ts_created=%d, ts_modified=%d, uri=%s)'\
            % (self.dat, self.id, self.ts_created, self.ts_modified, self.uri)

    def add_geometries(self, geometries):
        """Add list of geometries to this container.

        Args:
            geometries (list): List of geometries to add. Each geometry object is derived from the Geometry class. i.e Point, Polygon... Maximum 1000 items.

        Returns:
            list. List of Geometry objects.

        Raises:
            snowfloat.errors.RequestError

        Example:
        
        >>> points = [
        ...           snowfloat.geometries.Point(
        ...               coordinates=[p1x, p1y, p1z], ts=ts1, dat=dat1),
        ...           snowfloat.geometries.Point(
        ...               coordinates=[p2x, p2y, p2z], ts=ts2, dat=dat2)]
        >>> points = container.add_geometries(points)
        >>> print points[0]
        Point(id=6bf3f0bc551f41a6b6d435d51793c850,
              uri=/geo/1/containers/11d53e204a9b45299e68d186e7405779/geometries/6bf3f0bc551f41a6b6d435d51793c850
              coordinates=[p1x, p1y, p1z],
              ts=ts1,
              dat=dat1,
              ts_created=1358010636,
              ts_modified=1358010636)
        """
        uri = '%s/geometries' % (self.uri,)
        return snowfloat.geometry.add_geometries(uri, geometries)

    def get_geometries(self, type=None, ts_range=(0, None), query=None,
            geometry=None, **kwargs):
        """Returns container's geometries.

        Kwargs:
            type (str): Geometries type.
            
            ts_range (tuple): Geometries timestamps range.
            
            query (str): Distance or spatial query.
            
            geometry (Geometry): Geometry object for query lookup.

            distance (int): Distance in meters for some queries.

            spatial_operation (str): Spatial operation to run on each object returned.

        Returns:
            generator. Yields Geometry objects.
        
        Raises:
            snowfloat.errors.RequestError

        Example:
        
        >>> container.get_geometries(ts_range(ts1, ts2))

        or:

        >>> point = snowfloat.geometry.Point(px, py)
        >>> container.get_geometries(query=distance_lt,
                                     geometry=point,
                                     distance=10000)
        """
        for e in snowfloat.geometry.get_geometries(self.uri, type,
            ts_range, query, geometry, **kwargs):
            yield e

    def delete_geometries(self, type=None, ts_range=(0, None)):
        """Deletes container's geometries.

        Args:
            container_id (str): Container's ID.

        Kwargs:
            type (str): Geometries type.
            
            ts_range (tuple): Geometries timestamps range.

        Raises:
            snowfloat.errors.RequestError
        """
        if not ts_range[1]:
            end_time = time.time()
        else:
            end_time = ts_range[1]
        uri = '%s/geometries' % (self.uri)
        params = {'ts__gte': ts_range[0],
                  'ts__lte': end_time,
                 }
        if type:
            params['type__exact'] = type
        snowfloat.request.delete(uri, params)

    def delete_geometry(self, geometry_id):
        """Deletes a geometry.

        Args:
            geometry_id (str): Geometry's ID.

        Raises:
            snowfloat.errors.RequestError
        """
        uri = '%s/geometries/%s' % (self.uri, geometry_id)
        snowfloat.request.delete(uri)

    def update(self, **kwargs):
        """Edit container's attributes.

        Raises:
            snowfloat.errors.RequestError
        """
        for k, v in kwargs.items():
            setattr(self, k, v)
        snowfloat.request.put(self.uri,
            data=snowfloat.container.format_container(self))
        self.ts_modified = int(time.time())

    def delete(self):
        """Deletes this container.

        Raises:
            snowfloat.errors.RequestError
        """
        snowfloat.request.delete(self.uri)


def format_containers(containers):
    d = [format_container(c) for c in containers]
    
    return d

def format_container(c):
    return {'dat': c.dat}

def parse_containers(containers):
    return [Container(c['dat'], c['id'], c['ts_created'],
                  c['ts_modified'], c['uri']) for c in containers]

def update_container(cs, cd):
    cd.id = cs['id']
    cd.uri = cs['uri']
    cd.dat = cs['dat']
    cd.ts_created = cs['ts_created']
    cd.ts_modified = cs['ts_modified']
 
