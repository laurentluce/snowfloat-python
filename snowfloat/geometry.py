"""Geometries objects: Points, Polygons."""

import json
import sys
import time

try:
    import shapely.geometry
    POINT_CLS = shapely.geometry.Point
    POLYGON_CLS = shapely.geometry.Polygon
except ImportError:
    POINT_CLS = object
    POLYGON_CLS = object

import snowfloat.request

class Geometry(object):
    """Parent class of all geometries.

    Attributes:
        coordinates (list): List of coordinates.

        tag (str): Details about geometry. Maximum length: 256.

        uuid (str): UUID.

        uri (str): URI.

        geometry_ts: Timestamp.

        ts_created: Creation timestamp.

        ts_modified: Modification timestamp.

        geometry_type: Point or Polygon.

        container_uuid: Container's UUID.

        spatial: Attribute to store spatial operation result.
    """

    coordinates = None
    tag = None
    uuid = None
    uri = None
    geometry_ts = None
    ts_created = None
    ts_modified = None
    geometry_type = None
    container_uuid = None
    spatial = None

    def __init__(self, coordinates, **kwargs):
        for key, val in kwargs.items():
            getattr(self, key)
            setattr(self, key, val)
        self.coordinates = coordinates
        if not self.geometry_ts:
            self.geometry_ts = time.time()
        # spatial can be a geometry in the geojson format
        if self.spatial and isinstance(self.spatial, dict):
            thismodule = sys.modules[__name__]
            try:
                self.spatial = getattr(thismodule, self.spatial['type'])(
                    self.spatial['coordinates'])
            except AttributeError:
                raise
    
    def __str__(self):
        return '%s(coordinates=%s, tag=%s, ts=%s, uuid=%s, uri=%s, ' \
                'ts_created=%d, ts_modified=%d, geometry_type=%s, ' \
                'container_uuid=%s, spatial=%s' \
            % (self.__class__.__name__, self.coordinates, self.tag,
               self.geometry_ts,
               self.uuid, self.uri, self.ts_created, self.ts_modified,
               self.geometry_type, self.container_uuid, self.spatial)

    def num_points(self):
        """Returns the geometry number of points."""
        raise NotImplementedError()

    def update(self, **kwargs):
        """Update geometry's attributes.

        Raises:
            snowfloat.errors.RequestError
        """
        for key, value in kwargs.items():
            if key == 'type':
                key = 'geometry_type'
            setattr(self, key, value)
        snowfloat.request.put(self.uri,
            data=snowfloat.geometry.format_geometry(self))
        self.ts_modified = int(time.time())

    def delete(self):
        """Deletes a geometry.

        Raises:
            snowfloat.errors.RequestError
        """
        snowfloat.request.delete(self.uri)


def add_geometries(uri, geometries):
    """POST geometries to the server.

    Args:
        geometries (list): List of Geometry objects.

    Returns:
        list: List of Geometry objects stored.
    """
    res = snowfloat.request.post(uri, geometries,
        format_func=format_geometries)
    # convert list of json geometries to Geometry objects
    for i, feature in enumerate(res['features']):
        update_geometry(geometries[i], feature)

    return geometries

def get_geometries(uri, **kwargs):
    """GET geometries from the server.

    Kwargs:
        geometry_type (str): Geometries type.
        
        ts_range (tuple): Geometries timestamps range.
        
        query (str): Distance or spatial query.
        
        geometry (Geometry): Geometry object for query lookup.

        distance (int): Distance in meters for some queries.

        spatial_operation (str): Spatial operation to run on each object returned.

        spatial_geometry (Geometry): Geometry object for spatial operation.

    Returns:
        generator. Yield Geometry objects.
    """
    try:
        start_time, end_time = kwargs['ts_range']
    except KeyError:
        start_time = 0
        end_time = time.time()
    get_uri = '%s/geometries' % (uri,)
    params = {'geometry_ts__gte': start_time,
              'geometry_ts__lte': end_time}
    if 'geometry_type' in kwargs:
        params['geometry_type__exact'] = kwargs['geometry_type']

    if 'query' in kwargs:
        try:
            distance = kwargs['distance']
        except KeyError:
            distance = None
        geojson = {'type': kwargs['geometry'].geometry_type,
                   'coordinates': kwargs['geometry'].coordinates,
                   'properties': {
                       'distance': distance}
            }
        params['geometry__%s' % (kwargs['query'],)] = json.dumps(geojson)

    if 'spatial_operation' in kwargs:
        for key, value in kwargs.iteritems():
            if key.startswith('spatial_'):
                if key == 'spatial_geometry':
                    geojson = {'type': value.geometry_type,
                               'coordinates': value.coordinates}
                    params[key] = json.dumps(geojson)
                else:
                    params[key] = value

    for res in snowfloat.request.get(get_uri, params):
        # convert list of json geometries to Geometry objects
        geometries = parse_geometries(res['geo']['features'])
        for geometry in geometries:
            yield geometry

def parse_geometries(geometries):
    """Convert geometry dictionaries to Geometry objects.

    Args:
        geometries (list): Dictionaries.

    Returns:
        list: List of Point or Polygon objects.
    """
    thismodule = sys.modules[__name__]
    return [getattr(thismodule, g['geometry']['type'])(
                g['geometry']['coordinates'],
                tag=g['properties']['tag'],
                geometry_ts=g['properties']['geometry_ts'],
                uuid=g['id'],
                uri=g['properties']['uri'],
                ts_created=g['properties']['ts_created'],
                ts_modified=g['properties']['ts_modified'],
                spatial=g['properties']['spatial']) for g in geometries]

def format_geometries(geometries):
    """Format geojson dictionary using Geometry objects.

    Args:
        geometries (list): Geometry objects.

    Returns:
        str: geojson dictionary.
    """
    return {'type': 'FeatureCollection',
            'features': [format_geometry(g) for g in geometries]}
    
def format_geometry(geometry):
    """Format geojson dictionary using a Geometry object.

    Args:
        geometry (Geometry): Geometry object.

    Returns:
        str: geojson dictionary.
    """
    return {'type': 'Feature',
            'geometry': {'type': geometry.geometry_type,
                         'coordinates': geometry.coordinates},
            'properties': {
               'geometry_ts': geometry.geometry_ts,
               'tag': geometry.tag}
           }

def update_geometry(destination, source):
    """Update Geometry object from geojson.

    Args:
        destination: Geometry object to update.

        source: geojson source.
    """
    destination.uuid = source['id']
    destination.uri = source['properties']['uri']
    destination.container_uuid = destination.uri.split('/')[4]
    destination.tag = source['properties']['tag']
    destination.geometry_ts = source['properties']['geometry_ts']
    destination.ts_created = source['properties']['ts_created']
    destination.ts_modified = source['properties']['ts_modified']


class Point(Geometry, POINT_CLS):
    """Geometry Point.
    """

    geometry_type = 'Point'

    def __init__(self, coordinates, **kwargs):
        coords = coordinates
        if POINT_CLS != object:
            shapely.geometry.Point.__init__(self, coords)
        if len(coords) == 2:
            coords.append(0)
        if coords[2] == None:
            coords[2] = 0
        Geometry.__init__(self, coords, **kwargs)
    
    def num_points(self):
        """Geometry Point has one point."""
        return 1


class Polygon(Geometry, POLYGON_CLS):
    """Geometry Polygon."""

    geometry_type = 'Polygon'

    def __init__(self, coordinates, **kwargs):
        coords = coordinates
        if POLYGON_CLS != object:
            shapely.geometry.Polygon.__init__(self, coords[0])
        for coordinates in coords[0]:
            if len(coordinates) == 3 and coordinates[2] == None:
                coordinates[2] = 0
        if coords[0][0] != coords[0][-1]:
            coords[0].append(coords[0][0])
        Geometry.__init__(self, coords, **kwargs)

    def num_points(self):
        """Returns the number of points defining this polygon."""
        return len(self.coordinates[0])


