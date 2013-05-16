import json
import unittest
import random
import time

from mock import Mock, patch, call
import requests

import snowfloat.auth
import snowfloat.client
import snowfloat.errors
import snowfloat.settings

class Tests(unittest.TestCase):

    def setUp(self):
        self.client = snowfloat.client.Client()
        # login
        self.client.login(snowfloat.settings.USERNAME,
            snowfloat.settings.API_KEY)
        # remove containers
        self.client.delete_containers()

    def test_containers(self):

        # list containers, should be none
        containers = [e for e in self.client.get_containers()]
        self.assertListEqual(containers, [])

        # add containers
        containers = [snowfloat.container.Container(tag='test_tag_%d' % (i+1,))
            for i in range(10)]
        t = time.time()
        containers = self.client.add_containers(containers)
        print 'add containers: %.2f' % (time.time() - t)
        self.assertEqual(len(containers), 10)
        for i in range(10):
            self.assertEqual(containers[i].tag, 'test_tag_%d' % (i+1,))
        
        # list containers
        t = time.time()
        containers = [e for e in self.client.get_containers()]
        print 'get containers: %.2f' % (time.time() - t)
        self.assertEqual(len(containers), 10)
        for i in range(10):
            self.assertEqual(containers[i].tag, 'test_tag_%d' % (i+1,))

        # update container
        t = time.time()
        containers[0].update(tag='test_tag')
        print 'update container: %.2f' % (time.time() - t)
        containers = [e for e in self.client.get_containers()]
        self.assertEqual(containers[0].tag, 'test_tag')

        # delete a container
        t = time.time()
        containers[0].delete()
        print 'delete container: %.2f' % (time.time() - t)
        
        # list containers
        containers = [e for e in self.client.get_containers()]
        self.assertEqual(len(containers), 9)
        for i in range(9):
            self.assertEqual(containers[i].tag, 'test_tag_%d' % (i+2,))

        # delete containers
        self.client.delete_containers()
        
        # list containers
        containers = [e for e in self.client.get_containers()]
        self.assertEqual(len(containers), 0)

    def test_features_points(self):

        # add container
        containers = [snowfloat.container.Container(tag='test_tag_1')]
        containers = self.client.add_containers(containers)
        self.assertEqual(containers[0].tag, 'test_tag_1')

        # get features
        features = [e for e in self.client.get_features(
            containers[0].uuid, geometry_type='Point')]
        self.assertListEqual(features, [])

        # add features
        features_to_add = []
        for i in range(1000):
            point = snowfloat.geometry.Point(
                coordinates=[random.random() * 90,
                             random.random() * -90,
                             random.random() * 1000])
            fields = {'tag': 'test_tag_%d' % (i+1),
                      'ts': random.random() * 1000}
            feature = snowfloat.feature.Feature(point, fields=fields)
            features_to_add.append(feature)
        
        t = time.time()
        features = self.client.add_features(containers[0].uuid, features_to_add)
        print 'add features points: %.2f' % (time.time() - t)
        for i in range(1000):
            for j in range(3):
                self.assertAlmostEqual(features[i].geometry.coordinates[j],
                    features_to_add[i].geometry.coordinates[j], 8)
            self.assertEqual(features[i].geometry.geometry_type, 'Point')
            self.assertAlmostEqual(features[i].fields['ts'],
                features_to_add[i].fields['ts'], 2)
            self.assertEqual(features[i].fields['tag'],
                features_to_add[i].fields['tag'])

        # get features
        t = time.time()
        features = [e for e in self.client.get_features(containers[0].uuid,
            geometry_type='Point')]
        print 'get features points: %.2f' % (time.time() - t)
        self.assertEqual(len(features), 1000)
        for i in range(1000):
            for j in range(3):
                self.assertAlmostEqual(features[i].geometry.coordinates[j],
                    features_to_add[i].geometry.coordinates[j], 8)
            self.assertEqual(features[i].geometry.geometry_type, 'Point')
            self.assertAlmostEqual(features[i].fields['ts'],
                features_to_add[i].fields['ts'], 2)
            self.assertEqual(features[i].fields['tag'],
                features_to_add[i].fields['tag'])

        # update a feature
        t = time.time()
        point = snowfloat.geometry.Point(coordinates=[1, 2, 3])
        fields = {'tag': 'test_tag', 'ts': 10}
        features[0].update(geometry=point, fields=fields)
        print 'update feature point: %.2f' % (time.time() - t)
        points = [e for e in self.client.get_features(containers[0].uuid,
            geometry_type='Point')]
        self.assertListEqual(features[0].geometry.coordinates, [1, 2, 3])
        self.assertEqual(features[0].geometry.geometry_type, 'Point')
        self.assertEqual(features[0].fields['tag'], 'test_tag')
        self.assertEqual(features[0].fields['ts'], 10)

        # delete a point
        features[0].delete()

        # get features
        features = [e for e in self.client.get_features(containers[0].uuid,
            geometry_type='Point')]
        self.assertEqual(len(features), 999)
        
        # delete features
        t = time.time()
        self.client.delete_features(containers[0].uuid, geometry_type='Point')
        print 'delete features points: %.2f' % (time.time() - t)
        
        # get features
        features = [e for e in self.client.get_features(containers[0].uuid,
            geometry_type='Point')]
        self.assertListEqual(features, [])
  
        # add features
        features = []
        point = snowfloat.geometry.Point(coordinates=[45.0, 45.0, 0])
        fields = {'tag': 'test_tag_1', 'ts': 1}
        feature = snowfloat.feature.Feature(point, fields=fields)
        features.append(feature)
        point = snowfloat.geometry.Point(coordinates=[45.1, 45.0, 0])
        fields = {'tag': 'test_tag_2', 'ts': 2}
        feature = snowfloat.feature.Feature(point, fields=fields)
        features.append(feature)
        point = snowfloat.geometry.Point(coordinates=[55.0, 55.0, 0])
        fields = {'tag': 'test_tag_3', 'ts': 3}
        feature = snowfloat.feature.Feature(point, fields=fields)
        features.append(feature)
        features = self.client.add_features(containers[0].uuid, features)
 
        # get points using a spatial lookup
        point = snowfloat.geometry.Point([45.05, 45.0, 0])
        features = [e for e in self.client.get_features(containers[0].uuid,
            geometry_type='Point', query='distance_lte', geometry=point,
            distance=10000)]
        self.assertEqual(len(features), 2)
        self.assertListEqual(features[0].geometry.coordinates, [45.0, 45.0, 0])
        self.assertListEqual(features[1].geometry.coordinates, [45.1, 45.0, 0])
        
        # get points using a transform spatial operation
        features = [e for e in self.client.get_features(containers[0].uuid,
            geometry_type='Point', query='distance_lte', geometry=point,
            distance=10000, spatial_operation='transform',
            spatial_srid=32140)]
        self.assertEqual(len(features), 2)
        self.assertListEqual(features[0].geometry.coordinates,
            [9648070.041703206, 12327847.650520932, 0])
        self.assertListEqual(features[1].geometry.coordinates,
            [9650799.215247609, 12335585.450028483, 0])

    def test_features_polygons(self):

        # add container
        containers = [snowfloat.container.Container(tag='test_tag_1')]
        containers = self.client.add_containers(containers)
        self.assertEqual(containers[0].tag, 'test_tag_1')

        # get features polygons
        polygons = [e for e in self.client.get_features(
            containers[0].uuid, geometry_type='Polygon')]
        self.assertListEqual(polygons, [])

        # add features polygons
        features_to_add = []
        for i in range(1000):
            polygon = snowfloat.geometry.Polygon(
                coordinates=[[[0,
                               0,
                               0],
                              [random.random() * 90,
                               random.random() * -90,
                               random.random() * 1000],
                              [random.random() * 90,
                               random.random() * -90,
                               random.random() * 1000],
                              [0,
                               0,
                               0]
                             ]])
            fields = {'tag': 'test_tag_%d' % (i+1),
                      'ts': random.random() * 1000}
            feature = snowfloat.feature.Feature(polygon, fields=fields)
            features_to_add.append(feature)
        
        t = time.time()
        features = self.client.add_features(containers[0].uuid, features_to_add)
        print 'add features polygons: %.2f' % (time.time() - t)
        for i in range(1000):
            for j in range(4):
                for k in range(3):
                    self.assertAlmostEqual(
                        features[i].geometry.coordinates[0][j][k],
                        features_to_add[i].geometry.coordinates[0][j][k], 8)
            self.assertEqual(features[i].geometry.geometry_type, 'Polygon')
            self.assertAlmostEqual(features[i].fields['ts'],
                features_to_add[i].fields['ts'], 2)
            self.assertEqual(features[i].fields['tag'],
                features_to_add[i].fields['tag'])

        # get features polygons
        t = time.time()
        features = [e for e in self.client.get_features(containers[0].uuid,
            geometry_type='Polygon')]
        print 'get features polygons: %.2f' % (time.time() - t)
        self.assertEqual(len(features), 1000)
        for i in range(1000):
            for j in range(4):
                for k in range(3):
                    self.assertAlmostEqual(
                        features[i].geometry.coordinates[0][j][k],
                        features_to_add[i].geometry.coordinates[0][j][k], 8)
            self.assertEqual(features[i].geometry.geometry_type, 'Polygon')
            self.assertAlmostEqual(features[i].fields['ts'],
                features_to_add[i].fields['ts'], 2)
            self.assertEqual(features[i].fields['tag'],
                features_to_add[i].fields['tag'])
       
        # delete features polygons
        t = time.time()
        self.client.delete_features(containers[0].uuid,
            geometry_type='Polygon')
        print 'delete features polygons: %.2f' % (time.time() - t)
        
        # get features polygons
        features = [e for e in self.client.get_features(containers[0].uuid,
            geometry_type='Polygon')]
        self.assertListEqual(features, [])
  
        # add features polygons
        features = []
        polygon = snowfloat.geometry.Polygon(coordinates=[[[45.0, 45.0, 0],
                                                          [45.1, 45.0, 0],
                                                          [45.1, 45.1, 0],
                                                          [45.0, 45.0, 0]
                                                         ]])
        fields = {'tag': 'test_tag_1', 'ts': 1}
        feature = snowfloat.feature.Feature(polygon, fields=fields)
        features.append(feature)
        polygon = snowfloat.geometry.Polygon(coordinates=[[[45.05, 45.05, 0],
                                                          [45.06, 45.0, 0],
                                                          [45.06, 45.06, 0],
                                                          [45.05, 45.05, 0]
                                                         ]])
        fields = {'tag': 'test_tag_2', 'ts': 2}
        feature = snowfloat.feature.Feature(polygon, fields=fields)
        features.append(feature)
        polygon = snowfloat.geometry.Polygon(coordinates=[[[55.0, 55.0, 0],
                                                          [55.1, 55.0, 0],
                                                          [55.1, 55.1, 0],
                                                          [55.0, 55.0, 0]
                                                         ]])
        fields = {'tag': 'test_tag_3', 'ts': 3}
        feature = snowfloat.feature.Feature(polygon, fields=fields)
        features.append(feature)
        
        features = self.client.add_features(containers[0].uuid, features)
 
        # get polygons using a spatial lookup
        polygon = snowfloat.geometry.Polygon([[[45.0, 45.0, 0],
                                               [45.2, 45.0, 0],
                                               [45.2, 45.2, 0],
                                               [45.0, 45.0, 0]
                                       ]])
        features = [e for e in self.client.get_features(containers[0].uuid,
            geometry_type='Polygon', query='contained', geometry=polygon)]
        self.assertEqual(len(features), 2)
        self.assertListEqual(features[0].geometry.coordinates,
            [[[45.0, 45.0, 0],
              [45.1, 45.0, 0],
              [45.1, 45.1, 0],
              [45.0, 45.0, 0]
            ]])
        self.assertListEqual(features[1].geometry.coordinates,
            [[[45.05, 45.05, 0],
              [45.06, 45.0, 0],
              [45.06, 45.06, 0],
              [45.05, 45.05, 0]
            ]])

    def test_execute_tasks_distance(self):

        # add containers and points to compute on
        containers = [snowfloat.container.Container(tag='test_tag_1'),
                      snowfloat.container.Container(tag='test_tag_2')]
        containers = self.client.add_containers(containers)

        # add features points
        for i in range(2):
            features_to_add = []
            for j in range(1000):
                point = snowfloat.geometry.Point(
                    coordinates=[random.random() * 90,
                                 random.random() * -90,
                                 random.random() * 1000])
                fields = {'tag': 'test_tag_%d' % (j+1),
                          'ts': random.random() * 1000}
                feature = snowfloat.feature.Feature(point, fields=fields)
                features_to_add.append(feature)
 
            self.client.add_features(containers[i].uuid, features_to_add)

        tasks = [snowfloat.task.Task(
                    operation='distance',
                    resource='points',
                    container_uuid=containers[0].uuid),
                 snowfloat.task.Task(
                    operation='distance',
                    resource='points',
                    container_uuid=containers[1].uuid)]
        t = time.time()
        r = self.client.execute_tasks(tasks)
        print 'distance: %.2f' % (time.time() - t)
        self.assertEqual(r[0][0]['count'], 1000)
        self.assertGreater(r[0][0]['distance'], 0)
        self.assertIsNone(r[0][0]['velocity'])
        self.assertEqual(r[1][0]['count'], 1000)
        self.assertGreater(r[1][0]['distance'], 0)
        self.assertIsNone(r[1][0]['velocity'])

    def test_execute_tasks_map(self):

        # add containers and points to draw on the map
        containers = [snowfloat.container.Container(tag='test_tag_1'),
                      snowfloat.container.Container(tag='test_tag_2')]
        containers = self.client.add_containers(containers)

        # add features points
        for i in range(2):
            features_to_add = []
            for j in range(100):
                point = snowfloat.geometry.Point(
                    coordinates=[random.random() * 90,
                                 random.random() * -90,
                                 random.random() * 1000])
                fields = {'tag': 'test_tag_%d' % (j+1),
                          'ts': time.time()}
                feature = snowfloat.feature.Feature(point, fields=fields)
                features_to_add.append(feature)
 
            self.client.add_features(containers[i].uuid, features_to_add)

        tasks = [snowfloat.task.Task(
                    operation='map',
                    resource='points',
                    container_uuid=(containers[0].uuid, containers[1].uuid),
                    extras={'llcrnrlat': -75,
                            'llcrnrlon': -165,
                            'urcrnrlat': 75,
                            'urcrnrlon': 165})]
        t = time.time()
        r = self.client.execute_tasks(tasks)
        print 'map: %.2f' % (time.time() - t)
        self.assertTrue('url' in r[0][0])

    def test_execute_tasks_import_geospatial_data(self):

        tag_fields = ['dbl', 'int', 'str']
        geometry_ts_field = 'int'
        path = 'tests/test_point.zip'
        r = self.client.import_geospatial_data(path)
        self.assertDictEqual(r,
            {'containers_count': 1, 'features_count': 5})

    def test_usa(self):

        # add container
        containers = [snowfloat.container.Container(tag='World')]
        containers = self.client.add_containers(containers)

        # add cities
        features = []
        point = snowfloat.geometry.Point(coordinates=[-122.41941550000001,
                                                       37.7749295,
                                                       0])
        fields = {'tag': 'San Francisco', 'ts': 1}
        feature = snowfloat.feature.Feature(point, fields=fields)
        features.append(feature)
        
        point = snowfloat.geometry.Point(coordinates=[-104.98471790000002,
                                                       39.737567,
                                                       0])
        fields = {'tag': 'Denver', 'ts': 2}
        feature = snowfloat.feature.Feature(point, fields=fields)
        features.append(feature)
        
        point = snowfloat.geometry.Point(coordinates=[-71.0597732,
                                                       42.3584308,
                                                       0])
        fields = {'tag': 'Boston', 'ts': 3}
        feature = snowfloat.feature.Feature(point, fields=fields)
        features.append(feature)
                  
        point = snowfloat.geometry.Point(coordinates=[2.3522219000000177,
                                                      48.856614,
                                                      0])
        fields = {'tag': 'Paris', 'ts': 4}
        feature = snowfloat.feature.Feature(point, fields=fields)
        features.append(feature)
        
        features = self.client.add_features(containers[0].uuid, features)

        # add two polygons: west and east
        features = []
        poly_west = snowfloat.geometry.Polygon(
                        coordinates=[[[-125, 40, 0],
                                      [-125, 30, 0],
                                      [-100, 30, 0],
                                      [-100, 40, 0],
                                      [-125, 40, 0]]])
        fields = {'tag': 'West'}
        feature = snowfloat.feature.Feature(poly_west, fields=fields)
        features.append(feature)

        poly_east = snowfloat.geometry.Polygon(
                        coordinates=[[[-75, 45, 0],
                                      [-75, 35, 0],
                                      [-65, 35, 0],
                                      [-65, 45, 0],
                                      [-75, 45, 0]]])
        fields = {'tag': 'East'}
        feature = snowfloat.feature.Feature(poly_east, fields=fields)
        features.append(feature)
        
        features = self.client.add_features(containers[0].uuid, features)

        # look for points contained by the west polygon
        features = [e for e in self.client.get_features(containers[0].uuid,
            geometry_type='Point', query='contained', geometry=poly_west)]
        self.assertEqual(len(features), 2)
        self.assertListEqual(features[0].geometry.coordinates,
            [-122.4194155, 37.7749295, 0])
        self.assertListEqual(features[1].geometry.coordinates,
            [-104.9847179, 39.737567, 0])

        # look for points using distance_lt
        pl = snowfloat.geometry.Point(coordinates=[2.45, 48.85, 0])
        features = [e for e in self.client.get_features(containers[0].uuid,
            geometry_type='Point', query='distance_lt', geometry=pl,
            distance=20000)]
        self.assertEqual(len(features), 1)
        self.assertListEqual(features[0].geometry.coordinates,
            [2.3522219, 48.856614, 0.0])

        # spatial operation distance
        pl = snowfloat.geometry.Point(coordinates=[2.45, 48.85, 0])
        features = [e for e in self.client.get_features(containers[0].uuid,
            geometry_type='Point', spatial_operation='distance',
            spatial_geometry=pl)]
        self.assertEqual(len(features), 4)
        self.assertListEqual([f.spatial for f in features],
            [8958661.49177521, 7866626.32482402, 5538314.71584491,
             7191.65275350335])

        # task distance
        tasks = [snowfloat.task.Task(
                    operation='distance',
                    resource='points',
                    container_uuid=containers[0].uuid)]
        r = self.client.execute_tasks(tasks)
        self.assertListEqual(r,
            [[{u'count': 4, u'distance': 9722488.93059933,
               u'velocity': None}]])

        # task map
        tasks = [snowfloat.task.Task(
                    operation='map',
                    resource='points',
                    container_uuid=containers[0].uuid,
                    extras={'llcrnrlat': -75,
                            'llcrnrlon': -165,
                            'urcrnrlat': 75,
                            'urcrnrlon': 165})]
        r = self.client.execute_tasks(tasks)
        self.assertTrue('url' in r[0][0])


if __name__ == "__main__":
    unittest.main()
