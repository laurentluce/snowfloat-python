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

        Example:
    
        >>> containers = [snowfloat.container.Container(dat='Sally'),
                          snowfloat.container.Container(dat='Bob')]
        >>> containers = client.add_containers(containers)
        >>> print containers[0]
        Container(uuid=11d53e204a9b45299e68d186e7405779,
                  uri=/geo/1/containers/11d53e204a9b45299e68d186e7405779,
                  dat='Sally',
                  ts_created=1358100636,
                  ts_modified=1358100636)
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

    def add_geometries(self, container_uuid, geometries):
        """Add list of geometries to a container.

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
        ...               coordinates=[p2x, p2y, p2z], ts=ts2, dat=dat2),
        ...          ]
        >>> points = client.add_geometries(container_uuid, points)
        >>> print points[0]
        Point(uuid=6bf3f0bc551f41a6b6d435d51793c850,
              uri=/geo/1/containers/11d53e204a9b45299e68d186e7405779/geometries/6bf3f0bc551f41a6b6d435d51793c850
              coordinates=[p1x, p1y, p1z],
              ts=ts1,
              dat=dat1,
              ts_created=1358010636,
              ts_modified=1358010636)
        """
        uri = '%s/containers/%s/geometries' % (self.uri, container_uuid)
        return snowfloat.geometry.add_geometries(uri, geometries)

    def get_geometries(self, container_uuid, **kwargs):
        """Returns container's geometries.

        Args:
            container_uuid (str): Container's ID.

        Kwargs:
            geometry_type (str): Geometries type.

            ts_range (tuple): Points timestamps range.
            
            query (str): Spatial or distance query.
            
            geometry (Geometry): Geometry object for query lookup.
            
            distance (int): Distance in meters for some queries.

            spatial_operation (str): Spatial operation to run on each object returned.
        Returns:
            generator. Yields Geometry objects.
        
        Raises:
            snowfloat.errors.RequestError

        Example:
        
        >>> client.get_geometries(container_uuid, ts_range=(ts1, ts2))

        or:

        >>> point = snowfloat.geometry.Point([px, py])
        >>> client.get_geometries(container_uuid, query=distance_lt,
                                  geometry=point, distance=10000)
        """
        uri = '%s/containers/%s' % (self.uri, container_uuid)
        for geometry in snowfloat.geometry.get_geometries(uri, **kwargs):
            yield geometry

    def delete_geometries(self, container_uuid, geometry_type=None,
            ts_range=(0, None)):
        """Deletes container's geometries.

        Args:
            container_uuid (str): Container's ID.

        Kwargs:
            type (str): Geometries type
            
            ts_range (tuple): Geometries timestamps range.

        Raises:
            snowfloat.errors.RequestError
        """
        if not ts_range[1]:
            end_time = time.time()
        else:
            end_time = ts_range[1]
        uri = '%s/containers/%s/geometries' % (self.uri, container_uuid)
        params = {'geometry_ts__gte': ts_range[0],
                  'geometry_ts__lte': end_time,
                 }
        if geometry_type:
            params['geometry_type__exact'] = geometry_type
        snowfloat.request.delete(uri, params)

    def execute_tasks(self, tasks, interval=5):
        """Execute a list tasks.

        Args:
            tasks (list): List of tasks to execute. Maximum 10 items.
        
        Kwargs:
            interval (int): Check tasks status interval in seconds.

        Returns:
            list: List of list of dictionaries.

        Example:
        
        >>> tasks = [
        ...     snowfloat.task.Task(
        ...         operation='distance',
        ...         resource='points',
        ...         container_uuid=container1.uuid,
        ...         ts_range=(t1, t2)),
        ...     snowfloat.task.Task(
        ...         operation='distance',
        ...         resource='points',
        ...         container_uuid=container2.uuid,
        ...         ts_range=(t1, t2))]
        >>> r = self.client.execute_tasks(tasks)
        >>> r
        [[{"count": 10000, "distance": 38263, "velocity": 0.21}],
         [{"count": 10000, "distance": 14231, "velocity": 0.06}]]
 
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
                        results[task_uuid] = [json.loads(res.dat) 
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

    def import_geospatial_data(self, path, dat_fields=()):
        """Execute a list tasks.

        Args:
            path (str): OGR data archive path.
        
        Kwargs:
            dat_fields (tuple): List of fields to store in the attribute "dat".

        Returns:
            dict: Dictionary containing the number of containers and geometries added.

        Example:
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
                    extras={'blob_uuid': blob_uuid,
                            'dat_fields': dat_fields})]
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
        if task.ts_range:
            task_to_add['geometry_ts__gte'] = task.ts_range[0]
            task_to_add['geometry_ts__lte'] = task.ts_range[1]

        geometry_type = _convert_resource_to_type(task.resource)
        if geometry_type:
            task_to_add['geometry_type__exact'] = geometry_type

        if task.container_uuid:
            if (isinstance(task.container_uuid, list) or
                isinstance(task.container_uuid, tuple)):
                task_to_add['container__sid__in'] = task.container_uuid
            else:
                task_to_add['container__sid'] = task.container_uuid

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
