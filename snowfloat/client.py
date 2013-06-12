"""Snowfloat Client.

This is the first object to instantiate to interact with the API.
"""

import json
import time

import snowfloat.layer
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

    def add_layers(self, layers):
        """Add list of layers.

        Args:
            layers (list): List of Layer objects to add. Maximum 1000 items.

        Returns:
            list. List of Layer objects.

        Raises:
            snowfloat.errors.RequestError
        """
        uri = self.uri + '/layers'
        i = 0
        res = snowfloat.request.post(uri, layers,
            format_func=snowfloat.layer.format_layers)
        # convert list of json geometries to Geometry objects
        for layer in res:
            snowfloat.layer.update_layer(layer, layers[i])
            i += 1
        
        return layers

    def get_layers(self, **kwargs):
        """Returns all layers.

        Kwargs:
            Layer's attribute condition.

        Returns:
            list. List of Layer objects.
        
        Raises:
            snowfloat.errors.RequestError
        """
        uri = self.uri + '/layers'
        params = snowfloat.request.format_params(kwargs)
        layers = []
        for res in snowfloat.request.get(uri, params):
            # convert list of json layers to Layer objects
            layers.extend(snowfloat.layer.parse_layers(res['layers']))
        
        return layers

    def delete_layers(self):
        """Deletes all layers.

        Raises:
            snowfloat.errors.RequestError
        """
        uri = '%s/layers' % (self.uri,)
        snowfloat.request.delete(uri)

    def add_features(self, layer_uuid, features):
        """Add features to a layer.

        Args:
            features (list): List of features to add. Maximum 1000 items.

        Returns:
            list. List of Feature objects.

        Raises:
            snowfloat.errors.RequestError
        """
        uri = '%s/layers/%s/features' % (self.uri, layer_uuid)
        return snowfloat.feature.add_features(uri, features)

    def get_features(self, layer_uuid, **kwargs):
        """Returns layer's features.

        Args:
            layer_uuid (str): Layer's ID.

        Kwargs:
            geometry_type (str): Geometries type.

            query (str): Spatial or distance query.
            
            geometry (Geometry): Geometry object for query lookup.
            
            distance (int): Distance in meters for some queries.

            spatial_operation (str): Spatial operation to run on each object returned.
            
            spatial_geometry (Geometry): Geometry object for spatial operation.

            Field value condition.

        Returns:
            list. List of Feature objects.
        
        Raises:
            snowfloat.errors.RequestError
        """
        uri = '%s/layers/%s' % (self.uri, layer_uuid)
        return [feature for feature in snowfloat.feature.get_features(
            uri, **kwargs)]

    def delete_features(self, layer_uuid,
            **kwargs):
        """Deletes layer's features.

        Args:
            layer_uuid (str): Layer's ID.

        Kwargs:
            Field value condition.

            Feature's attribute condition.

        Raises:
            snowfloat.errors.RequestError
        """
        params = {}
        params.update(snowfloat.request.format_params(kwargs))
 
        uri = '%s/layers/%s/features' % (self.uri, layer_uuid)
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

    def import_geospatial_data(self, path, srs=None):
        """Import geospatial data.

        Args:
            path (str): OGR data archive path.

            srs (dict): Spatial reference system to replace the one in the data source file.
        
        Returns:
            dict: Dictionary containing the number of layers and features added.
        """
        # add blob with the data source content
        uri = '%s/blobs' % (self.uri)
        with open(path) as archive:
            res = snowfloat.request.post(uri, archive, serialize=False)
        blob_uuid = res['uuid']

        # make sure the blob is in the success state
        uri = '%s/blobs/%s' % (self.uri, blob_uuid)
        while True:
            res = [e for e in snowfloat.request.get(uri)]
            if res[0]['state'] == 'started':
                time.sleep(5)
            else:
                if res[0]['state'] == 'failure':
                    raise snowfloat.errors.RequestError(status=500, 
                        code=None, message='Upload failed.', more=None)
                break

        # execute import data source task
        extras={'blob_uuid': blob_uuid}
        if srs:
            extras['srs'] = srs
        tasks = [snowfloat.task.Task(
                    operation='import_geospatial_data',
                    extras=extras)]
        res = self.execute_tasks(tasks)
        
        # delete blob
        uri = '%s/blobs/%s' % (self.uri, blob_uuid)
        snowfloat.request.delete(uri)

        if 'error' in res[0]:
            raise snowfloat.errors.RequestError(status=400, 
                code=2, message=res[0]['error'], more=None)

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
        task_to_add = {'operation': task.operation}

        if task.layer_uuid:
            if (isinstance(task.layer_uuid, list) or
                isinstance(task.layer_uuid, tuple)):
                task_to_add['layer__uuid__in'] = task.layer_uuid
            else:
                task_to_add['layer__uuid__exact'] = task.layer_uuid

        if task.extras:
            task_to_add['extras'] = task.extras

        tasks_to_process.append(task_to_add)

    return tasks_to_process

