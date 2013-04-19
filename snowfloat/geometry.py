import json
import sys
import time

try:
    import shapely.geometry
    point_cls = shapely.geometry.Point
    polygon_cls = shapely.geometry.Polygon
except ImportError:
    point_cls = object
    polygon_cls = object


import snowfloat.request


class Geometry(object):

    coordinates = None
    dat = None
    id = None
    uri = None
    ts = None
    ts_created = None
    ts_modified = None
    type = None
    container_id = None
    spatial = None

    def __init__(self, coordinates, dat=None, ts=None, id=None, uri=None,
            ts_created=None, ts_modified=None, spatial=None,
            container_id=None):
        self.coordinates = coordinates
        self.dat = dat
        self.id = id
        self.uri = uri
        if not ts:
            self.ts = time.time()
        else:
            self.ts = ts
        self.ts_created = ts_created
        self.ts_modified = ts_modified
        # spatial can be a geometry in the geojson format
        if isinstance(spatial, dict):
            thismodule = sys.modules[__name__]
            self.spatial = getattr(thismodule, spatial['type'])(
                spatial['coordinates'])
        else:
            self.spatial = spatial 
    
    def __str__(self):
        return '%s(coordinates=%s, dat=%s, ts=%s, id=%s, uri=%s, ' \
                'ts_created=%d, ts_modified=%d)' \
            % (self.__class__.__name__, self.coordinates, self.dat, self.ts,
               self.id, self.uri, self.ts_created, self.ts_modified)

    def update(self, **kwargs):
        """Edit geometry's attributes.

        Raises:
            snowfloat.errors.RequestError
        """
        for k, v in kwargs.items():
            setattr(self, k, v)
        snowfloat.request.post(self.uri,
            data=snowfloat.geometry.format_geometry(self))
        self.ts_modified = int(time.time())

    def delete(self):
        """Deletes a geometry.

        Raises:
            snowfloat.errors.RequestError
        """
        snowfloat.request.delete(self.uri)


def add_geometries(uri, geometries):
    i = 0
    for r in snowfloat.request.put(uri, geometries,
            format_func=format_geometries):
        # convert list of json geometries to Geometry objects
        for f in r['features']:
            update_geometry(geometries[i], f)
            i += 1

    return geometries

def get_geometries(uri, type, ts_range, query, geometry, **kwargs):
    if not ts_range[1]:
        end_time = time.time()
    else:
        end_time = ts_range[1]
    u = '%s/geometries' % (uri,)
    params = {'ts__gte': ts_range[0],
              'ts__lte': end_time}
    if type:
        params['type__exact'] = type

    if query:
        try:
            distance = kwargs['distance']
        except KeyError:
            distance = None
        e = {'type': geometry.type,
             'coordinates': geometry.coordinates,
             'properties': {
                'distance': distance}
            }
        params['geometry__%s' % (query,)] = json.dumps(e)

    if 'spatial_operation' in kwargs:
        for k, v in kwargs.iteritems():
            if k.startswith('spatial_'):
                if k == 'spatial_geometry':
                    e = {'type': v.type, 'coordinates': v.coordinates}
                    params[k] = json.dumps(e)
                else:
                    params[k] = v

    for r in snowfloat.request.get(u, params):
        # convert list of json geometries to Geometry objects
        geometries = parse_geometries(r['geo']['features'])
        for p in geometries:
            yield p

def parse_geometries(geometries):
    thismodule = sys.modules[__name__]
    return [getattr(thismodule, g['geometry']['type'])(
                g['geometry']['coordinates'],
                g['properties']['dat'],
                g['properties']['ts'],
                g['id'],
                g['properties']['uri'],
                g['properties']['ts_created'],
                g['properties']['ts_modified'],
                g['properties']['spatial']) for g in geometries]

def format_geometries(geometries):
    d = {'type': 'FeatureCollection',
         'features': [format_geometry(g) for g in geometries]}
    
    return d

def format_geometry(g):
    return {'type': 'Feature',
            'geometry': {'type': g.type, 'coordinates': g.coordinates},
            'properties': {
               'ts': g.ts,
               'dat': g.dat}
           }

def update_geometry(g, f):
    g.id = f['id']
    g.uri = f['properties']['uri']
    g.container_id = g.uri.split('/')[4]
    g.dat = f['properties']['dat']
    g.ts = f['properties']['ts']
    g.ts_created = f['properties']['ts_created']
    g.ts_modified = f['properties']['ts_modified']


class Point(Geometry, point_cls):
    
    type = 'Point'

    def __init__(self, coordinates, dat=None, ts=None, id=None, uri=None,
            ts_created=None, ts_modified=None, spatial=None,
            container_id=None):
        if point_cls != object:
            shapely.geometry.Point.__init__(self, *coordinates)
        coords = coordinates
        if len(coords) == 2:
            coords.append(0)
        if coords[2] == None:
            coords[2] = 0
        Geometry.__init__(self, coords, dat, ts, id, uri, ts_created,
            ts_modified, spatial, container_id)


class Polygon(Geometry, polygon_cls):
    
    type = 'Polygon'

    def __init__(self, coordinates, dat=None, ts=None, id=None, uri=None,
            ts_created=None, ts_modified=None, spatial=None,
            container_id=None):
        if polygon_cls != object:
            shapely.geometry.Polygon.__init__(self, coordinates[0])
        coords = coordinates
        for c in coords[0]:
            if len(c) == 3 and c[2] == None:
                c[2] = 0
        if coords[0][0] != coords[0][-1]:
            coords[0].append(coords[0][0])
        Geometry.__init__(self, coords, dat, ts, id, uri, ts_created,
            ts_modified, spatial, container_id)


