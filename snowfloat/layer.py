"""Layer of geometries."""

import time

import snowfloat.feature
import snowfloat.request

class Layer(object):

    """Layer of geometries.

    Attributes:
        name (str): Name of the layer. Maximum length: 256.

        uuid (str): UUID.

        uri (str): URI.
        
        ts_created (int): Creation timestamp.
        
        ts_modified (int): Modification timestamp.
    """
    name = ''
    uuid = None
    uri = None
    ts_created = None
    ts_modified = None

    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            getattr(self, key)
            setattr(self, key, val)

    
    def __str__(self):
        return 'Layer(name=%s, uuid=%s, ts_created=%d, ts_modified=%d, '\
               'uri=%s)'\
            % (self.name, self.uuid, self.ts_created, self.ts_modified,
               self.uri)

    def add_features(self, features):
        """Add list of features to this layer.

        Args:
            geometries (list): List of features to add. Maximum 1000 items.

        Returns:
            list. List of Feature objects.

        Raises:
            snowfloat.errors.RequestError
        """
        uri = '%s/features' % (self.uri,)
        return snowfloat.feature.add_features(uri, features)

    def get_features(self, **kwargs):
        """Returns layer's features.

        Kwargs:
            geometry_type (str): Geometries type.
            
            query (str): Distance or spatial query.
            
            geometry (Geometry): Geometry object for query lookup.

            distance (int): Distance in meters for some queries.

            spatial_operation (str): Spatial operation to run on each object returned.

            spatial_geometry (Geometry): Geometry object for spatial operation.

            field_...: Field value condition.

        Returns:
            generator. Yields Feature objects.
        
        Raises:
            snowfloat.errors.RequestError
        """
        for res in snowfloat.feature.get_features(self.uri, **kwargs):
            yield res

    def delete_features(self, **kwargs):
        """Deletes layer's features.

        Kwargs:
            geometry_type (str): Geometries type.
           
            field_...: Field value condition.

        Raises:
            snowfloat.errors.RequestError
        """
        params = {}
        for key, val in kwargs.items():
            if key.startswith('field_'):
                s1 = key[:key.index('_')]
                s2 = key[key.index('_')+1:key.rindex('_')]
                s3 = key[key.rindex('_')+1:]
                params[s1 + '__' + s2 + '__' + s3] = val
 
        if 'geometry_type' in kwargs:
            params['geometry_type__exact'] = kwargs['geometry_type']
        
        uri = '%s/features' % (self.uri)
        snowfloat.request.delete(uri, params)

    def delete_geometry(self, uuid):
        """Deletes a geometry.

        Args:
            uuid (str): Geometry's uuid.

        Raises:
            snowfloat.errors.RequestError
        """
        uri = '%s/geometries/%s' % (self.uri, uuid)
        snowfloat.request.delete(uri)

    def update(self, **kwargs):
        """Update layer's attributes.

        Raises:
            snowfloat.errors.RequestError
        """
        for key, value in kwargs.items():
            setattr(self, key, value)
        snowfloat.request.put(self.uri,
            data=snowfloat.layer.format_layer(self))
        self.ts_modified = int(time.time())

    def delete(self):
        """Deletes this layer.

        Raises:
            snowfloat.errors.RequestError
        """
        snowfloat.request.delete(self.uri)


def format_layers(layers):
    """Format layers to be sent to the server.

    Args:
        layers (list): List of Layer objects.

    Returns:
        list: List of layer dictionaries to be sent to the server.
    """
    return [format_layer(layer) for layer in layers]
    
def format_layer(layer):
    """Format layer to be sent to the server.

    Args:
        layer (Layer): Layer object.

    Returns:
        dict: Layer dictionary to be sent to the server.
    """
    return {'name': layer.name}

def parse_layers(layers):
    """Convert layer dictionaries.

    Args:
        layers (list): List of layer dictionaries.

    Returns:
        list: List of Layer objects.
    """
    return [Layer(
                name=layer['name'],
                uuid=layer['uuid'],
                ts_created=layer['ts_created'],
                ts_modified=layer['ts_modified'],
                uri=layer['uri']) for layer in layers]

def update_layer(layer_source, layer_destination):
    """Update Layer object from a layer dictionary.

    Args:
        layer_source: Layer dictionary.

        layer_destination: Layer object.
    """
    layer_destination.uuid = layer_source['uuid']
    layer_destination.uri = layer_source['uri']
    layer_destination.name = layer_source['name']
    layer_destination.ts_created = layer_source['ts_created']
    layer_destination.ts_modified = layer_source['ts_modified']
 
