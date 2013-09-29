"""Layer of geometries."""

import snowfloat.feature
import snowfloat.request

class Layer(object):

    """Layer of geometries.

    Attributes:
        name (str): Name of the layer. Maximum length: 256.

        uuid (str): UUID.

        uri (str): URI.
        
        date_created (str): Creation date in ISO format.

        date_modified (str): Modification date in ISO format.

        fields (list): List of fields definitions.

        srid (int): Spatial reference system SRID code.

        dims (int): Spatial reference system number of dimensions.

        extent (list): Spatial extent list. (xmin, xmax, ymin, ymax).
    """
    name = ''
    uuid = None
    uri = None
    date_created = None
    date_modified = None
    num_features = 0
    num_points = 0
    fields = None
    srid = None
    dims = None
    extent = None

    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            getattr(self, key)
            setattr(self, key, val)
    
    def __str__(self):
        return 'Layer: name=%s, uuid=%s, date_created=%s, date_modified=%s, '\
               'uri=%s, num_features=%d, num_points=%d, fields=%s, '\
               'srid=%d, dims=%d, extent=%s' \
            % (self.name, self.uuid, self.date_created, self.date_modified,
               self.uri, self.num_features, self.num_points, self.fields,
               self.srid, self.dims, self.extent)

    def __repr__(self):
        return 'Layer(name=%r, uuid=%r, date_created=%r, date_modified=%r, '\
               'uri=%r, num_features=%r, num_points=%r, fields=%r, '\
               'srid=%r, dims=%r, extent=%r)' \
            % (self.name, self.uuid, self.date_created, self.date_modified,
               self.uri, self.num_features, self.num_points, self.fields,
               self.srid, self.dims, self.extent)

    def add_features(self, features):
        """Add list of features to this layer.

        Args:
            features (list): List of features to add. Maximum 1000 items.

        Returns:
            list. List of Feature objects.

        Raises:
            snowfloat.errors.RequestError
        """
        uri = '%s/features' % (self.uri,)
        res = snowfloat.feature.add_features(uri, features)
        
        self.num_features += len(features)
        self.num_points += \
            sum([feature.geometry.num_points() for feature in features])

        return res

    def get_features(self, **kwargs):
        """Returns layer's features.

        Kwargs:
            geometry_type (str): Geometries type.
            
            query (str): Distance or spatial query.
            
            geometry (Geometry): Geometry object for query lookup.

            distance (int): Distance in meters for some queries.

            spatial_operation (str): Spatial operation to run on each object returned.

            spatial_geometry (Geometry): Geometry object for spatial operation.

            Field value condition.

            order_by (tuple): Tuple of attributes and fields to order by.

            query_slice (tuple): Tuple to limit entries returned.

        Returns:
            list. List of Feature objects.
        
        Raises:
            snowfloat.errors.RequestError
        """
        return [res for res in snowfloat.feature.get_features(
            self.uri, **kwargs)]

    def delete_features(self, **kwargs):
        """Deletes layer's features.

        Kwargs:
            Feature's attribute condition.
           
            Field value condition.

        Raises:
            snowfloat.errors.RequestError
        """
        params = {}
        params.update(snowfloat.request.format_params(kwargs))
 
        uri = '%s/features' % (self.uri)
        res = snowfloat.request.delete(uri, params)
        
        self.num_features -= res['num_features']
        self.num_points -= res['num_points']

    def delete_feature(self, uuid):
        """Deletes a feature.

        Args:
            uuid (str): Feature's uuid.

        Raises:
            snowfloat.errors.RequestError
        """
        uri = '%s/features/%s' % (self.uri, uuid)
        res = snowfloat.request.delete(uri)
        
        self.num_features -= 1
        self.num_points -= res['num_points']

    def update(self, **kwargs):
        """Update layer's attributes.

        Raises:
            snowfloat.errors.RequestError
        """
        layer = Layer()
        for key, value in kwargs.items():
            getattr(self, key)
            setattr(layer, key, value)
        snowfloat.request.put(self.uri,
            data=snowfloat.layer.format_layer(layer))
        # if success: update self attributes
        for key, value in kwargs.items():
            setattr(self, key, value)

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
    layer_formatted = {'name': layer.name}
    if layer.fields:
        layer_formatted['fields'] = layer.fields
    if layer.srid:
        layer_formatted['srid'] = layer.srid
    if layer.dims:
        layer_formatted['dims'] = layer.dims
    if layer.extent:
        layer_formatted['extent'] = layer.extent
    
    return layer_formatted

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
                date_created=layer['date_created'],
                date_modified=layer['date_modified'],
                uri=layer['uri'],
                num_features=layer['num_features'],
                num_points=layer['num_points'],
                fields=layer['fields'],
                srid=layer['srid'],
                dims=layer['dims'],
                extent=layer['extent']
                ) for layer in layers]

def update_layer(layer_source, layer_destination):
    """Update Layer object from a layer dictionary.

    Args:
        layer_source: Layer dictionary.

        layer_destination: Layer object.
    """
    layer_destination.uuid = layer_source['uuid']
    layer_destination.uri = layer_source['uri']
    layer_destination.name = layer_source['name']
    layer_destination.date_created = layer_source['date_created']
    layer_destination.date_modified = layer_source['date_modified']
 
