"""Snowfloat Client.

This is the first object to instantiate to interact with the API.
"""

import json
import time

import snowfloat.container
import snowfloat.auth
import snowfloat.errors
import snowfloat.request
import snowfloat.result
import snowfloat.task

class Client(object):
    """API client.

    Attributes:
        uri (str): URI prefix to use for requests.
    """
    uri = '/geo/1'

    def __init__(self):
        pass

    def login(self, username, key):
        """Login to the server and store a session ID locally.

        Args:
            username (str): Username to use to login.

            key (str): API key to use to login.

        Raises:
            snowfloat.errors.RequestError
        """
        res = snowfloat.request.post(self.uri + '/login',
            {'username': username, 'key': key})
        snowfloat.auth.session_uuid = res['more'] 

    def add_containers(self, containers):
        """Add list of containers.

        Args:
            containers (list): List of Container objects to add. Maximum 1000 items.

        Returns:
            list. List of Container objects.

        Raises:
            snowfloat.errors.RequestError
        """
        uri = self.uri + '/containers'
        i = 0
        res = snowfloat.request.post(uri, containers,
            format_func=snowfloat.container.format_containers)
        # convert list of json geometries to Geometry objects
        for container in res:
            snowfloat.container.update_container(container, containers[i])
            i += 1
        
        return containers

    def get_containers(self):
        """Returns all containers.

        Returns:
            generator. Yields Container objects.
        
        Raises:
            snowfloat.errors.RequestError
        """
        uri = self.uri + '/containers'
        for res in snowfloat.request.get(uri):
            # convert list of json containers to Container objects
            containers = snowfloat.container.parse_containers(res['containers'])
            for container in containers:
                yield container

    def delete_containers(self):
        """Deletes all containers.

        Raises:
            snowfloat.errors.RequestError
        """
        uri = '%s/containers' % (self.uri,)
        snowfloat.request.delete(uri)

    def add_features(self, container_uuid, features):
        """Add features to a container.

        Args:
            features (list): List of features to add. Maximum 1000 items.

        Returns:
            list. List of Feature objects.

        Raises:
            snowfloat.errors.RequestError
        """
        uri = '%s/containers/%s/features' % (self.uri, container_uuid)
        return snowfloat.feature.add_features(uri, features)

    def get_features(self, container_uuid, **kwargs):
        """Returns container's features.

        Args:
            container_uuid (str): Container's ID.

        Kwargs:
            geometry_type (str): Geometries type.

            query (str): Spatial or distance query.
            
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
        uri = '%s/containers/%s' % (self.uri, container_uuid)
        for feature in snowfloat.feature.get_features(uri, **kwargs):
            yield feature

    def delete_features(self, container_uuid,
            **kwargs):
        """Deletes container's features.

        Args:
            container_uuid (str): Container's ID.

        Kwargs:
            geometry_type (str): Geometries type
            
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
        
        uri = '%s/containers/%s/features' % (self.uri, container_uuid)
        snowfloat.request.delete(uri, params)

    def execute_tasks(self, tasks, interval=5):
        """Execute a list tasks.

        Args:
            tasks (list): List of tasks to execute. Maximum 10 items.
        
        Kwargs:
            interval (int): Check tasks status interval in seconds.

        Returns:
            list: List of list of dictionaries.

        """
        tasks_to_process = _prepare_tasks(tasks)
        tasks_to_process = self._add_tasks(tasks_to_process)
        # poll tasks state until they are done
        task_uuids = [task.uuid for task in tasks_to_process]
        results = {}
        for task_uuid in task_uuids:
            results[task_uuid] = None
        try:
            while task_uuids:
                task_done_uuids = []
                for task_uuid in task_uuids:
                    task = self._get_task(task_uuid)
                    if task.state == 'success':
                        # get results
                        results[task_uuid] = [json.loads(res.tag) 
                            for res in self._get_results(task_uuid)]
                        task_done_uuids.append(task_uuid)
                    elif task.state == 'failure':
                        results[task_uuid] = {'error': task.reason}
                        task_done_uuids.append(task_uuid)
               
                for task_uuid in task_done_uuids:
                    task_uuids.remove(task_uuid)
                if task_uuids:
                    time.sleep(interval)
        except snowfloat.errors.RequestError:
            pass

        return [results[task.uuid] for task in tasks_to_process]

    def import_geospatial_data(self, path):
        """Import geospatial data.

        Args:
            path (str): OGR data archive path.
        
        Returns:
            dict: Dictionary containing the number of containers and features added.
        """
        # add blob with the data source content
        uri = '%s/blobs' % (self.uri)
        with open(path) as archive:
            res = snowfloat.request.post(uri, archive, serialize=False)
        blob_uuid = res['uuid']
        
        # execute import data source task
        tasks = [snowfloat.task.Task(
                    operation='import_geospatial_data',
                    resource='geometries',
                    extras={'blob_uuid': blob_uuid})]
        res = self.execute_tasks(tasks)

        # delete blob
        uri = '%s/blobs/%s' % (self.uri, blob_uuid)
        snowfloat.request.delete(uri)

        return res[0][0]

    def _add_tasks(self, tasks):
        """Send tasks to the server.

        Args:
            tasks (list): List of task dictionaries to send to the server.
        """
        uri = '%s/tasks' % (self.uri)
        res = snowfloat.request.post(uri, tasks)
        return snowfloat.task.parse_tasks(res)

    def _get_task(self, task_uuid):
        """Get task from server.

        Args:
            task_uuid (str): Task UUID.
        """
        uri = '%s/tasks/%s' % (self.uri, task_uuid)
        res = [e for e in snowfloat.request.get(uri)]
        task = snowfloat.task.parse_tasks(res)[0]
        return task

    def _get_results(self, task_uuid):
        """Get task results from server.

        Args:
            task_uuid (str): Task UUID.

        Returns:
            generator: Yields Result objects.
        """
        uri = '%s/tasks/%s/results' % (self.uri, task_uuid)
        for res in snowfloat.request.get(uri):
            # convert list of json results to Result objects
            results = snowfloat.result.parse_results(res['results'])
            for res in results:
                yield res


def _prepare_tasks(tasks):
    """Prepare list of tasks to send to the server based on Task objects passed.

    Args:
        tasks (list): List of Task objects to parse.

    Returns:
        list: List of task dictionaries to send to the server.
    """
    tasks_to_process = []
    for task in tasks:
        # add task
        task_to_add = {'operation': task.operation,
                       'resource': task.resource}

        geometry_type = _convert_resource_to_type(task.resource)
        if geometry_type:
            task_to_add['geometry_type__exact'] = geometry_type

        if task.container_uuid:
            if (isinstance(task.container_uuid, list) or
                isinstance(task.container_uuid, tuple)):
                task_to_add['container__uuid__in'] = task.container_uuid
            else:
                task_to_add['container__uuid'] = task.container_uuid

        if task.extras:
            task_to_add['extras'] = task.extras

        tasks_to_process.append(task_to_add)

    return tasks_to_process


def _convert_resource_to_type(resource):
    """Convert task resource string to geometry type string.

    Args:
        resource (str): Task resource.

    Returns:
        str: Geometry type.
    """
    if resource == 'geometries':
        return None
    else:
        return resource[:-1].capitalize() 
