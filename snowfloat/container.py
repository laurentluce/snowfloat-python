"""Container of geometries."""

import time

import snowfloat.feature
import snowfloat.request

class Container(object):

    """Container of geometries.

    Attributes:
        tag (str): Details about the container. Maximum length: 256.

        uuid (str): UUID.

        uri (str): URI.
        
        ts_created (int): Creation timestamp.
        
        ts_modified (int): Modification timestamp.
    """
    tag = ''
    uuid = None
    uri = None
    ts_created = None
    ts_modified = None

    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            getattr(self, key)
            setattr(self, key, val)

    
    def __str__(self):
        return 'Container(tag=%s, uuid=%s, ts_created=%d, ts_modified=%d, '\
               'uri=%s)'\
            % (self.tag, self.uuid, self.ts_created, self.ts_modified, self.uri)

    def add_features(self, features):
        """Add list of features to this container.

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
        """Returns container's features.

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

    def delete_features(self, geometry_type=None, **kwargs):
        """Deletes container's features.

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
 
        if geometry_type:
            params['geometry_type__exact'] = geometry_type
        
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
        """Update container's attributes.

        Raises:
            snowfloat.errors.RequestError
        """
        for key, value in kwargs.items():
            setattr(self, key, value)
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
    """Format containers to be sent to the server.

    Args:
        containers (list): List of Container objects.

    Returns:
        list: List of container dictionaries to be sent to the server.
    """
    return [format_container(c) for c in containers]
    
def format_container(container):
    """Format container to be sent to the server.

    Args:
        container (Container): Container object.

    Returns:
        dict: Container dictionary to be sent to the server.
    """
    return {'tag': container.tag}

def parse_containers(containers):
    """Convert container dictionaries.

    Args:
        containers (list): List of container dictionaries.

    Returns:
        list: List of Container objects.
    """
    return [Container(
                tag=c['tag'],
                uuid=c['uuid'],
                ts_created=c['ts_created'],
                ts_modified=c['ts_modified'],
                uri=c['uri']) for c in containers]

def update_container(container_source, container_destination):
    """Update Container object from a container dictionary.

    Args:
        container_source: Container dictionary.

        container_destination: Container object.
    """
    container_destination.uuid = container_source['uuid']
    container_destination.uri = container_source['uri']
    container_destination.tag = container_source['tag']
    container_destination.ts_created = container_source['ts_created']
    container_destination.ts_modified = container_source['ts_modified']
 
