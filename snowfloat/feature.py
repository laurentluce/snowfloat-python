"""Layer Features objects"""

import json
import sys
import time

import snowfloat.geometry
import snowfloat.request

class Feature(object):
    """Layer's features class.

    Attributes:
        uuid (str): UUID.

        uri (str): URI.

        ts_created (int): Creation timestamp.

        ts_modified (int): Modification timestamp.

        geometry (Geometry): Geometry.

        fields (dict): Feature's fields.

        container_uuid (str): Container's UUID.

        spatial: Attribute to store spatial operation result.
    """

    uuid = None
    uri = None
    ts_created = None
    ts_modified = None
    geometry = None
    fields = {}
    container_uuid = None
    spatial = None

    def __init__(self, geometry, fields=None, **kwargs):
        for key, val in kwargs.items():
            getattr(self, key)
            setattr(self, key, val)
        self.geometry = geometry
        if fields:
            self.fields = fields
        # spatial can be a geometry in the geojson format
        if self.spatial and isinstance(self.spatial, dict):
            try:
                self.spatial = getattr(snowfloat.geometry,
                    self.spatial['type'])(
                        self.spatial['coordinates'])
            except AttributeError:
                raise
    
    def __str__(self):
        return '%s(uuid=%s, uri=%s, ' \
                'ts_created=%d, ts_modified=%d, ' \
                'geometry=%s, fields=%s ' \
                'container_uuid=%s, spatial=%s' \
            % (self.__class__.__name__,
               self.uuid, self.uri, self.ts_created, self.ts_modified,
               self.geometry, self.fields,
               self.container_uuid, self.spatial)

    def update(self, **kwargs):
        """Update feature's attributes.

        Raises:
            snowfloat.errors.RequestError
        """
        for key, value in kwargs.items():
            setattr(self, key, value)
        snowfloat.request.put(self.uri,
            data=snowfloat.feature.format_feature(self))
        self.ts_modified = int(time.time())

    def delete(self):
        """Deletes a feature.

        Raises:
            snowfloat.errors.RequestError
        """
        snowfloat.request.delete(self.uri)


def add_features(uri, features):
    """POST features to the server.

    Args:
        features (list): List of Feature objects.

    Returns:
        list: List of Feature objects stored.
    """
    res = snowfloat.request.post(uri, features,
        format_func=format_features)
    # convert list of json features to Feature objects
    for i, feature in enumerate(res['features']):
        update_feature(features[i], feature)

    return features

def get_features(uri, **kwargs):
    """GET features from the server.

    Kwargs:
        geometry_type (str): Geometries type.
        
        query (str): Distance or spatial query.
        
        geometry (Geometry): Geometry object for query lookup.

        distance (int): Distance in meters for some queries.

        spatial_operation (str): Spatial operation to run on each object returned.

        spatial_geometry (Geometry): Geometry object for spatial operation.

        field_...: Field value condition.

    Returns:
        generator. Yield Feature objects.
    """
    get_uri = '%s/features' % (uri,)

    params = {}
    for key, val in kwargs.items():
        if key.startswith('field_'):
            s1 = key[:key.index('_')]
            s2 = key[key.index('_')+1:key.rindex('_')]
            s3 = key[key.rindex('_')+1:]
            params[s1 + '__' + s2 + '__' + s3] = val
            
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
        # convert list of json features to Feature objects
        features = parse_features(res['geo']['features'])
        for feature in features:
            yield feature

def parse_features(features):
    """Convert feature dictionaries to Feature objects.

    Args:
        features (list): Dictionaries.

    Returns:
        list: List of Feature objects.
    """
    thismodule = sys.modules[__name__]
    res = []
    for feature in features:
        feature_to_add = Feature(
            getattr(snowfloat.geometry, feature['geometry']['type'])(
                feature['geometry']['coordinates']),
            uuid=feature['id'],
            uri=feature['properties']['uri'],
            ts_created=feature['properties']['ts_created'],
            ts_modified=feature['properties']['ts_modified'],
            spatial=feature['properties']['spatial'])

        fields = {}
        for key, val in feature['properties'].items():
            if key.startswith('field_'):
                fields[key[6:]] = val
        feature_to_add.fields = fields

        res.append(feature_to_add)

    return res

def format_features(features):
    """Format geojson dictionary using Feature objects.

    Args:
        features (list): Feature objects.

    Returns:
        str: geojson dictionary.
    """
    return {'type': 'FeatureCollection',
            'features': [format_feature(f) for f in features]}
    
def format_feature(feature):
    """Format geojson dictionary using a Feature object.

    Args:
        feature (Feature): Feature object.

    Returns:
        str: geojson dictionary.
    """
    return {'type': 'Feature',
            'geometry': {'type': feature.geometry.geometry_type,
                         'coordinates': feature.geometry.coordinates},
            'properties': {'field_%s' % (key,): val
                for key, val in feature.fields.items()}
           }

def update_feature(destination, source):
    """Update Feature object from geojson.

    Args:
        destination: Feature object to update.

        source: geojson source.
    """
    destination.uuid = source['id']
    destination.uri = source['properties']['uri']
    destination.container_uuid = destination.uri.split('/')[4]
    destination.ts_created = source['properties']['ts_created']
    destination.ts_modified = source['properties']['ts_modified']


