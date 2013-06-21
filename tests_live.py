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
        # remove layers
        self.client.delete_layers()

    def test_layers(self):

        # list layers, should be none
        layers = self.client.get_layers()
        self.assertListEqual(layers, [])

        # add layers
        layers = [snowfloat.layer.Layer(name='test_tag_%d' % (i+1,),
                                        fields=[{'name': 'field_%d' % (i+1,),
                                                 'type': 'string',
                                                 'size': 256},],
                                        srs={'type': 'EPSG',
                                             'properties':
                                                {'code': 4326, 'dim': 3}},
                                        extent=[1, 2, 3, 4])
            for i in range(10)]
        t = time.time()
        layers = self.client.add_layers(layers)
        print 'add layers: %.2f' % (time.time() - t)
        self.assertEqual(len(layers), 10)
        for i in range(10):
            self.assertEqual(layers[i].name, 'test_tag_%d' % (i+1,))
            self.assertListEqual(layers[i].fields,
                [{'name': 'field_%d' % (i+1,), 'type': 'string',
                  'size': 256},])
            self.assertDictEqual(layers[i].srs,
                {'type': 'EPSG', 'properties': {'code': 4326, 'dim': 3}})
            self.assertListEqual(layers[i].extent, [1, 2, 3, 4])
        
        # list layers
        t = time.time()
        layers = self.client.get_layers()
        print 'get layers: %.2f' % (time.time() - t)
        self.assertEqual(len(layers), 10)
        for i in range(10):
            self.assertEqual(layers[i].name, 'test_tag_%d' % (i+1,))
            self.assertListEqual(layers[i].fields,
                [{'name': 'field_%d' % (i+1,), 'type': 'string',
                  'size': 256},])
            self.assertDictEqual(layers[i].srs,
                {'type': 'EPSG', 'properties': {'code': 4326, 'dim': 3}})
            self.assertListEqual(layers[i].extent, [1, 2, 3, 4])

        # update layer
        t = time.time()
        layers[0].update(name='test_tag')
        print 'update layer: %.2f' % (time.time() - t)
        layers = self.client.get_layers()
        self.assertEqual(layers[0].name, 'test_tag')

        # delete a layer
        t = time.time()
        layers[0].delete()
        print 'delete layer: %.2f' % (time.time() - t)
        
        # list layers
        layers = self.client.get_layers()
        self.assertEqual(len(layers), 9)
        for i in range(9):
            self.assertEqual(layers[i].name, 'test_tag_%d' % (i+2,))
            self.assertListEqual(layers[i].fields,
                [{'name': 'field_%d' % (i+2,), 'type': 'string',
                  'size': 256},])
            self.assertDictEqual(layers[i].srs,
                {'type': 'EPSG', 'properties': {'code': 4326, 'dim': 3}})
            self.assertListEqual(layers[i].extent, [1, 2, 3, 4])

        # delete layers
        self.client.delete_layers()
        
        # list layers
        layers = self.client.get_layers()
        self.assertEqual(len(layers), 0)

    def test_features_points(self):

        # add layer
        layers = [snowfloat.layer.Layer(name='test_tag_1',
                                        fields=[{'name': 'tag',
                                                 'type': 'string',
                                                 'size': 256},
                                                {'name': 'ts',
                                                 'type': 'real'},],
                                        srs={'type': 'EPSG',
                                             'properties':
                                                {'code': 4326, 'dim': 3}}),]
        layers = self.client.add_layers(layers)

        # get features
        features = self.client.get_features(
            layers[0].uuid, geometry_type_exact='Point')
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
        features = self.client.add_features(layers[0].uuid, features_to_add)
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
        features = self.client.get_features(layers[0].uuid,
            geometry_type_exact='Point')
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
        points = self.client.get_features(layers[0].uuid,
            geometry_type_exact='Point')
        self.assertListEqual(features[0].geometry.coordinates, [1, 2, 3])
        self.assertEqual(features[0].geometry.geometry_type, 'Point')
        self.assertEqual(features[0].fields['tag'], 'test_tag')
        self.assertEqual(features[0].fields['ts'], 10)

        # delete a point
        features[0].delete()

        # get features
        features = self.client.get_features(layers[0].uuid,
            geometry_type_exact='Point')
        self.assertEqual(len(features), 999)
        
        # delete features
        t = time.time()
        self.client.delete_features(layers[0].uuid,
            geometry_type_exact='Point')
        print 'delete features points: %.2f' % (time.time() - t)
        
        # get features
        features = self.client.get_features(layers[0].uuid,
            geometry_type_exact='Point')
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
        features = self.client.add_features(layers[0].uuid, features)
 
        # get points using a spatial lookup
        point = snowfloat.geometry.Point([45.05, 45.0, 0])
        features = self.client.get_features(layers[0].uuid,
            geometry_type_exact='Point', query='distance_lte', geometry=point,
            distance=10000)
        self.assertEqual(len(features), 2)
        self.assertListEqual(features[0].geometry.coordinates, [45.0, 45.0, 0])
        self.assertListEqual(features[1].geometry.coordinates, [45.1, 45.0, 0])
        
        # get points using a transform spatial operation
        features = self.client.get_features(layers[0].uuid,
            geometry_type_exact='Point', query='distance_lte', geometry=point,
            distance=10000, spatial_operation='transform',
            spatial_srid=32140)
        self.assertEqual(len(features), 2)
        self.assertListEqual(features[0].geometry.coordinates,
            [9648070.041703206, 12327847.650520932, 0])
        self.assertListEqual(features[1].geometry.coordinates,
            [9650799.215247609, 12335585.450028483, 0])

    def test_features_polygons(self):

        # add layer
        layers = [snowfloat.layer.Layer(name='test_tag_1',
                                        fields=[{'name': 'tag',
                                                 'type': 'string',
                                                 'size': 256},
                                                {'name': 'ts',
                                                 'type': 'real'},],
                                        srs={'type': 'EPSG',
                                             'properties':
                                                {'code': 4326, 'dim': 3}}),]
        layers = self.client.add_layers(layers)

        # get features polygons
        polygons = self.client.get_features(
            layers[0].uuid, geometry_type_exact='Polygon')
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
        features = self.client.add_features(layers[0].uuid, features_to_add)
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
        features = self.client.get_features(layers[0].uuid,
            geometry_type_exact='Polygon')
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
        self.client.delete_features(layers[0].uuid,
            geometry_type_exact='Polygon')
        print 'delete features polygons: %.2f' % (time.time() - t)
        
        # get features polygons
        features = self.client.get_features(layers[0].uuid,
            geometry_type_exact='Polygon')
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
        
        features = self.client.add_features(layers[0].uuid, features)
 
        # get polygons using a spatial lookup
        polygon = snowfloat.geometry.Polygon([[[45.0, 45.0, 0],
                                               [45.2, 45.0, 0],
                                               [45.2, 45.2, 0],
                                               [45.0, 45.0, 0]
                                       ]])
        features = self.client.get_features(layers[0].uuid,
            geometry_type_exact='Polygon', query='contained',
            geometry=polygon)
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

    def test_execute_tasks_map(self):

        # add layers and points to draw on the map
        layers = [snowfloat.layer.Layer(name='test_tag_1',
                                        fields=[{'name': 'tag',
                                                 'type': 'string',
                                                 'size': 256},
                                                {'name': 'ts',
                                                 'type': 'real'},],
                                        srs={'type': 'EPSG',
                                             'properties':
                                                {'code': 4326, 'dim': 3}}),]
        layers = self.client.add_layers(layers)

        # add features points
        features_to_add = []
        for i in range(100):
            point = snowfloat.geometry.Point(
                coordinates=[random.random() * 90,
                             random.random() * -90,
                             random.random() * 1000])
            fields = {'tag': 'test_tag_1',
                      'ts': time.time()}
            feature = snowfloat.feature.Feature(point, fields=fields)
            features_to_add.append(feature)

        self.client.add_features(layers[0].uuid, features_to_add)

        tasks = [snowfloat.task.Task(
                    operation='map',
                    task_filter = {
                        'layer_uuid_exact': layers[0].uuid},
                    spatial = {
                        'operation': 'transform', 'srid': 4326},
                    extras={'xlim': [-165, 165],
                            'ylim': [-75, 75]})]
        t = time.time()
        r = self.client.execute_tasks(tasks)
        print 'map: %.2f' % (time.time() - t)
        self.assertTrue('url' in r[0][0])

    def test_execute_tasks_import_geospatial_data(self):

        path = 'tests/test_point.zip'
        srs={'type': 'EPSG',
             'properties':
                {'code': 4326}}
        r = self.client.import_geospatial_data(path, srs)
        self.assertDictEqual(r,
            {'layers_count': 1, 'features_count': 5})

    def test_execute_tasks_import_geospatial_data_mi_forest_roads(self):

        path = 'tests/mi_forest_roads.zip'
        r = self.client.import_geospatial_data(path)
        self.assertDictEqual(r,
            {'layers_count': 1, 'features_count': 3671})
        
        layers = self.client.get_layers()
        self.assertEqual(len(layers), 1)
        layer = layers[0]
        
        tasks = [snowfloat.task.Task(
            operation='map',
            task_filter = {
                'layer_uuid_exact': layer.uuid},
            spatial = {
                'operation': 'transform', 'srid': 4326})]
                    
        t = time.time()
        r = self.client.execute_tasks(tasks)
        print 'map: %.2f' % (time.time() - t)
        self.assertTrue('url' in r[0][0])
 
        features = layer.get_features(order_by=('field_name',))
        self.assertEqual(len(features), 3671)
        self.assertListEqual([f.fields['name'] for f in features[5:10]],
            [u'2008 SPUR M12E',
             u'2010A SPUR R93B',
             u'2010 SPUR R93C',
             u'2036 SPUR R120L',
             u'2041 SPUR R116D'])
        features = layer.get_features(spatial_operation='length')
        self.assertEqual(len(features), 3671)
        self.assertListEqual([f.spatial for f in features[:10]],
            [833.228853280628,
             578.312883521976,
             8278.66192812599,
             63.7906336300756,
             1288.71910814592,
             1714.5922385602,
             231.215921489381,
             3176.51957020512,
             122.116921566437,
             470.569520880827])

    def test_usa(self):

        # add layer
        layers = [snowfloat.layer.Layer(name='World',
                                        fields=[{'name': 'tag',
                                                 'type': 'string',
                                                 'size': 256},
                                                {'name': 'ts',
                                                 'type': 'real'},],
                                        srs={'type': 'EPSG',
                                             'properties':
                                                {'code': 4326, 'dim': 3}}),]
        layers = self.client.add_layers(layers)

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
        
        features = self.client.add_features(layers[0].uuid, features)

        # add two polygons: west and east
        features = []
        poly_west = snowfloat.geometry.Polygon(
                        coordinates=[[[-125, 40, 0],
                                      [-125, 30, 0],
                                      [-100, 30, 0],
                                      [-100, 40, 0],
                                      [-125, 40, 0]]])
        fields = {'tag': 'West', 'ts': 5}
        feature = snowfloat.feature.Feature(poly_west, fields=fields)
        features.append(feature)

        poly_east = snowfloat.geometry.Polygon(
                        coordinates=[[[-75, 45, 0],
                                      [-75, 35, 0],
                                      [-65, 35, 0],
                                      [-65, 45, 0],
                                      [-75, 45, 0]]])
        fields = {'tag': 'East', 'ts': 6}
        feature = snowfloat.feature.Feature(poly_east, fields=fields)
        features.append(feature)
        
        features = self.client.add_features(layers[0].uuid, features)

        # look for points contained by the west polygon
        features = self.client.get_features(layers[0].uuid,
            geometry_type_exact='Point', query='contained',
            geometry=poly_west)
        self.assertEqual(len(features), 2)
        self.assertListEqual(features[0].geometry.coordinates,
            [-122.4194155, 37.7749295, 0])
        self.assertListEqual(features[1].geometry.coordinates,
            [-104.9847179, 39.737567, 0])

        # look for points using distance_lt
        pl = snowfloat.geometry.Point(coordinates=[2.45, 48.85, 0])
        features = self.client.get_features(layers[0].uuid,
            geometry_type_exact='Point', query='distance_lt', geometry=pl,
            distance=20000)
        self.assertEqual(len(features), 1)
        self.assertListEqual(features[0].geometry.coordinates,
            [2.3522219, 48.856614, 0.0])

        # spatial operation distance
        pl = snowfloat.geometry.Point(coordinates=[2.45, 48.85, 0])
        features = self.client.get_features(layers[0].uuid,
            geometry_type_exact='Point', spatial_operation='distance',
            spatial_geometry=pl)
        self.assertEqual(len(features), 4)
        distances = [f.spatial for f in features]
        self.assertAlmostEqual(distances[0], 8958661.491775, 6)
        self.assertAlmostEqual(distances[1], 7866626.324824, 6)
        self.assertAlmostEqual(distances[2], 5538314.715845, 6)
        self.assertAlmostEqual(distances[3], 7191.652754, 6)


if __name__ == "__main__":
    unittest.main()
