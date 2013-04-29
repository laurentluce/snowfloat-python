import json
import time

import requests

import snowfloat.container
import snowfloat.auth
import snowfloat.errors
import snowfloat.request
import snowfloat.result
import snowfloat.task

class Client(object):

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
        r = snowfloat.request.post(self.uri + '/login',
            {'username': username, 'key': key})
        snowfloat.auth.session_id = r['more'] 

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
        Container(id=11d53e204a9b45299e68d186e7405779,
                  uri=/geo/1/containers/11d53e204a9b45299e68d186e7405779,
                  dat='Sally',
                  ts_created=1358100636,
                  ts_modified=1358100636)
        """
        uri = self.uri + '/containers'
        i = 0
        r = snowfloat.request.post(uri, containers,
            format_func=snowfloat.container.format_containers)
        # convert list of json geometries to Geometry objects
        for c in r:
            snowfloat.container.update_container(c, containers[i])
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
        for r in snowfloat.request.get(uri):
            # convert list of json containers to Container objects
            containers = snowfloat.container.parse_containers(r['containers'])
            for c in containers:
                yield c

    def delete_containers(self):
        """Deletes all containers.

        Raises:
            snowfloat.errors.RequestError
        """
        uri = '%s/containers' % (self.uri,)
        snowfloat.request.delete(uri)

    def add_geometries(self, container_id, geometries):
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
        >>> points = client.add_geometries(container_id, points)
        >>> print points[0]
        Point(id=6bf3f0bc551f41a6b6d435d51793c850,
              uri=/geo/1/containers/11d53e204a9b45299e68d186e7405779/geometries/6bf3f0bc551f41a6b6d435d51793c850
              coordinates=[p1x, p1y, p1z],
              ts=ts1,
              dat=dat1,
              ts_created=1358010636,
              ts_modified=1358010636)
        """
        uri = '%s/containers/%s/geometries' % (self.uri, container_id)
        return snowfloat.geometry.add_geometries(uri, geometries)

    def get_geometries(self, container_id, type=None, ts_range=(0, None),
            query=None, geometry=None, **kwargs):
        """Returns container's geometries.

        Args:
            container_id (str): Container's ID.

        Kwargs:
            type (str): Geometries type.

            ts_range (tuple): Points timestamps range.
            
            query (str): Spatial or distance query.
            
            geometry (Geometry): Geometry object for query lookup.
            
            distance (int): Distance in meters for some queries.

        Returns:
            generator. Yields Geometry objects.
        
        Raises:
            snowfloat.errors.RequestError

        Example:
        
        >>> client.get_geometries(container_id, ts_range(ts1, ts2))

        or:

        >>> point = snowfloat.geometry.Point([px, py])
        >>> client.get_geometries(container_id, query=distance_lt,
                                  geometry=point, distance=10000)
        """
        uri = '%s/containers/%s' % (self.uri, container_id)
        for e in snowfloat.geometry.get_geometries(uri, type, ts_range,
            query, geometry, **kwargs):
            yield e

    def delete_geometries(self, container_id, type=None, ts_range=(0, None)):
        """Deletes container's geometries.

        Args:
            container_id (str): Container's ID.

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
        uri = '%s/containers/%s/geometries' % (self.uri, container_id)
        params = {'ts__gte': ts_range[0],
                  'ts__lte': end_time,
                 }
        if type:
            params['type__exact'] = type
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
        ...         container_id=container1.id,
        ...         ts_range=(t1, t2)),
        ...     snowfloat.task.Task(
        ...         operation='distance',
        ...         resource='points',
        ...         container_id=container2.id,
        ...         ts_range=(t1, t2))]
        >>> r = self.client.execute_tasks(tasks)
        >>> r
        [[{"count": 10000, "distance": 38263, "velocity": 0.21}],
         [{"count": 10000, "distance": 14231, "velocity": 0.06}]]
 
        """
        tks = []
        for t in tasks:
            # add task
            task = {'operation': t.operation,
                    'resource': t.resource}
            if t.ts_range:
                task['ts__gte'] = t.ts_range[0]
                task['ts__lte'] = t.ts_range[1]

            type = self._convert_resource_to_type(t.resource)
            if type:
                task['type__exact'] = type

            if t.container_id:
                if (isinstance(t.container_id, list) or
                    isinstance(t.container_id, tuple)):
                    task['container__sid__in'] = t.container_id
                else:
                    task['container__sid'] = t.container_id

            if t.extras:
                task['extras'] = t.extras

            tks.append(task)
        tks = self._add_tasks(tks)
        # poll tasks state until they are done
        task_ids = [t.id for t in tks]
        results = {}
        for tid in task_ids:
            results[tid] = None
        try:
            while task_ids:
                task_done_ids = []
                for tid in task_ids:
                    t = self._get_task(tid)
                    if t.state == 'success':
                        # get results
                        results[tid] = [json.loads(r.dat) 
                            for r in self._get_results(tid)]
                        task_done_ids.append(tid)
                    elif t.state == 'failure':
                        results[tid] = {'error': t.reason}
                        task_done_ids.append(tid)
               
                for e in task_done_ids:
                    task_ids.remove(e)
                if task_ids:
                    time.sleep(interval)
        except snowfloat.errors.RequestError:
            pass

        return [results[t.id] for t in tks]

    def import_geospatial_data(self, path, dat_fields=()):
        """Execute a list tasks.

        Args:
            paths (str): OGR data archive path.
        
        Kwargs:
            dat_fields (tuple): List of fields to store in the attribute "dat".

        Returns:
            dict: Dictionary containing the number of containers and geometries added.

        Example:
        """
        # add blob with the data source content
        uri = '%s/blobs' % (self.uri)
        with open(path) as f:
            r = snowfloat.request.post(uri, f, serialize=False)
        blob_id = r['id']
        
        # execute import data source task
        tasks = [snowfloat.task.Task(
                    operation='import_geospatial_data',
                    resource='geometries',
                    extras={'blob_id': blob_id,
                            'dat_fields': dat_fields})]
        r = self.execute_tasks(tasks)

        # delete blob
        uri = '%s/blobs/%s' % (self.uri, blob_id)
        snowfloat.request.delete(uri)

        return r[0][0]

    def _add_tasks(self, data):
        uri = '%s/tasks' % (self.uri)
        r = snowfloat.request.post(uri, data)
        return snowfloat.task.parse_tasks(r)

    def _get_task(self, task_id):
        uri = '%s/tasks/%s' % (self.uri, task_id)
        r = [e for e in snowfloat.request.get(uri)]
        task = snowfloat.task.parse_tasks(r)[0]
        return task

    def _get_results(self, task_id):
        uri = '%s/tasks/%s/results' % (self.uri, task_id)
        for r in snowfloat.request.get(uri):
            # convert list of json results to Result objects
            results = snowfloat.result.parse_results(r['results'])
            for r in results:
                yield r

    def _convert_resource_to_type(self, resource):
        if resource == 'geometries':
            return None
        else:
            return resource[:-1].capitalize() 
