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

        date_created (str): Creation date in ISO format.

        date_modified (str): Modification date in ISO format.

        geometry (Geometry): Geometry.

        fields (dict): Feature's fields.

        layer_uuid (str): Layer's UUID.

        spatial: Attribute to store spatial operation result.
    """

    uuid = None
    uri = None
    date_created = None
    date_modified = None
    geometry = None
    fields = {}
    layer_uuid = None
    spatial = None
    num_features = 0
    num_points = 0

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
                'date_created=%s, date_modified=%s, ' \
                'geometry=%s, fields=%s ' \
                'layer_uuid=%s, spatial=%s' \
            % (self.__class__.__name__,
               self.uuid, self.uri, self.date_created, self.date_modified,
               self.geometry, self.fields,
               self.layer_uuid, self.spatial)

    def update(self, **kwargs):
        """Update feature's attributes.

        Raises:
            snowfloat.errors.RequestError
        """
        for key, value in kwargs.items():
            setattr(self, key, value)
        snowfloat.request.put(self.uri,
            data=snowfloat.feature.format_feature(self))

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
        query (str): Distance or spatial query.
        
        geometry (Geometry): Geometry object for query lookup.

        distance (int): Distance in meters for some queries.

        spatial_operation (str): Spatial operation to run on each object returned.

        spatial_geometry (Geometry): Geometry object for spatial operation.

        Field value condition.

        Feature's attribute condition.

    Returns:
        generator. Yield Feature objects.
    """
    get_uri = '%s/features' % (uri,)

    params = snowfloat.request.format_fields_params(kwargs)
            
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

    exclude = ('query', 'distance', 'geometry')
    params.update(snowfloat.request.format_params(kwargs, exclude=exclude))
    
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
            date_created=feature['properties']['date_created'],
            date_modified=feature['properties']['date_modified'],
            spatial=feature['properties']['spatial'],
            layer_uuid = feature['properties']['uri'].split('/')[4])

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
    destination.layer_uuid = destination.uri.split('/')[4]
    destination.date_created = source['properties']['date_created']
    destination.date_modified = source['properties']['date_modified']


