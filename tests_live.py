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

    def test_points(self):

        # add container
        containers = [snowfloat.container.Container(tag='test_tag_1')]
        containers = self.client.add_containers(containers)
        self.assertEqual(containers[0].tag, 'test_tag_1')

        # get points
        points = [e for e in self.client.get_geometries(
            containers[0].uuid, geometry_type='Point')]
        self.assertListEqual(points, [])

        # add points
        pts = [snowfloat.geometry.Point(coordinates=[random.random() * 90,
                                                     random.random() * -90,
                                                     random.random() * 1000],
                                        tag='test_tag_%d' % (i+1),
                                        geometry_ts=random.random() * 1000)
            for i in range(1000)]
        t = time.time()
        points = self.client.add_geometries(containers[0].uuid, pts)
        print 'add points: %.2f' % (time.time() - t)
        for i in range(1000):
            self.assertAlmostEqual(points[i].coordinates[0],
                pts[i].coordinates[0], 8)
            self.assertAlmostEqual(points[i].coordinates[1],
                pts[i].coordinates[1], 8)
            self.assertAlmostEqual(points[i].coordinates[2],
                pts[i].coordinates[2], 8)
            self.assertEqual(points[i].geometry_type, 'Point')
            self.assertAlmostEqual(points[i].geometry_ts, pts[i].geometry_ts, 2)
            self.assertEqual(points[i].tag, pts[i].tag)

        # get points
        t = time.time()
        points = [e for e in self.client.get_geometries(containers[0].uuid,
            geometry_type='Point')]
        print 'get points: %.2f' % (time.time() - t)
        self.assertEqual(len(points), 1000)
        for i in range(1000):
            self.assertAlmostEqual(points[i].coordinates[0],
                pts[i].coordinates[0], 8)
            self.assertAlmostEqual(points[i].coordinates[1],
                pts[i].coordinates[1], 8)
            self.assertAlmostEqual(points[i].coordinates[2],
                pts[i].coordinates[2], 8)
            self.assertEqual(points[i].geometry_type, 'Point')
            self.assertAlmostEqual(points[i].geometry_ts, pts[i].geometry_ts, 2)
            self.assertEqual(points[i].tag, pts[i].tag)
     
        # update a point
        t = time.time()
        points[0].update(coordinates=[1, 2, 3], tag='test_tag', geometry_ts=10)
        print 'update point: %.2f' % (time.time() - t)
        points = [e for e in self.client.get_geometries(containers[0].uuid,
            geometry_type='Point')]
        self.assertListEqual(points[0].coordinates, [1, 2, 3])
        self.assertEqual(points[0].geometry_type, 'Point')
        self.assertEqual(points[0].tag, 'test_tag')
        self.assertEqual(points[0].geometry_ts, 10)

        # delete a point
        points[0].delete()

        # get points
        points = [e for e in self.client.get_geometries(containers[0].uuid,
            geometry_type='Point')]
        self.assertEqual(len(points), 999)
        
        # delete points
        t = time.time()
        self.client.delete_geometries(containers[0].uuid, geometry_type='Point')
        print 'delete points: %.2f' % (time.time() - t)
        
        # get points
        points = [e for e in self.client.get_geometries(containers[0].uuid,
            geometry_type='Point')]
        self.assertListEqual(points, [])
  
        # add points
        pts = [snowfloat.geometry.Point(coordinates=[45.0, 45.0, 0],
                                        tag='test_tag_1',
                                        geometry_ts=1),
               snowfloat.geometry.Point(coordinates=[45.1, 45.0, 0],
                                        tag='test_tag_2',
                                        geometry_ts=2),
               snowfloat.geometry.Point(coordinates=[55.0, 55.0, 0],
                                        tag='test_tag_3',
                                        geometry_ts=3)]
        points = self.client.add_geometries(containers[0].uuid, pts)
 
        # get points using a spatial lookup
        point = snowfloat.geometry.Point([45.05, 45.0, 0])
        points = [e for e in self.client.get_geometries(containers[0].uuid,
            geometry_type='Point', query='distance_lte', geometry=point,
            distance=10000)]
        self.assertEqual(len(points), 2)
        self.assertListEqual(points[0].coordinates, [45.0, 45.0, 0])
        self.assertListEqual(points[1].coordinates, [45.1, 45.0, 0])
        
        # get points using a transform spatial operation
        points = [e for e in self.client.get_geometries(containers[0].uuid,
            geometry_type='Point', query='distance_lte', geometry=point,
            distance=10000, spatial_operation='transform',
            spatial_srid=32140)]
        self.assertEqual(len(points), 2)
        self.assertListEqual(points[0].coordinates,
            [9648070.041703206, 12327847.650520932, 0])
        self.assertListEqual(points[1].coordinates,
            [9650799.215247609, 12335585.450028483, 0])

    def test_polygons(self):

        # add container
        containers = [snowfloat.container.Container(tag='test_tag_1')]
        containers = self.client.add_containers(containers)
        self.assertEqual(containers[0].tag, 'test_tag_1')

        # get polygons
        polygons = [e for e in self.client.get_geometries(
            containers[0].uuid, geometry_type='Polygon')]
        self.assertListEqual(polygons, [])

        # add polygons
        pgs = [snowfloat.geometry.Polygon(coordinates=[[[0,
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
                                                      ]],
                                    tag='test_tag_%d' % (i+1),
                                    geometry_ts=random.random() * 1000)
            for i in range(1000)]
        t = time.time()
        polygons = self.client.add_geometries(containers[0].uuid, pgs)
        print 'add polygons: %.2f' % (time.time() - t)
        for i in range(1000):
            for j in range(4):
                self.assertAlmostEqual(polygons[i].coordinates[0][j][0],
                    pgs[i].coordinates[0][j][0], 8)
                self.assertAlmostEqual(polygons[i].coordinates[0][j][1],
                    pgs[i].coordinates[0][j][1], 8)
                self.assertAlmostEqual(polygons[i].coordinates[0][j][2],
                    pgs[i].coordinates[0][j][2], 8)
            self.assertEqual(polygons[i].geometry_type, 'Polygon')
            self.assertAlmostEqual(polygons[i].geometry_ts, pgs[i].geometry_ts, 2)
            self.assertEqual(polygons[i].tag, pgs[i].tag)

        # get polygons
        t = time.time()
        polygons = [e for e in self.client.get_geometries(containers[0].uuid,
            geometry_type='Polygon')]
        print 'get polygons: %.2f' % (time.time() - t)
        self.assertEqual(len(polygons), 1000)
        for i in range(1000):
            for j in range(4):
                self.assertAlmostEqual(polygons[i].coordinates[0][j][0],
                    pgs[i].coordinates[0][j][0], 8)
                self.assertAlmostEqual(polygons[i].coordinates[0][j][1],
                    pgs[i].coordinates[0][j][1], 8)
                self.assertAlmostEqual(polygons[i].coordinates[0][j][2],
                    pgs[i].coordinates[0][j][2], 8)
            self.assertEqual(polygons[i].geometry_type, 'Polygon')
            self.assertAlmostEqual(polygons[i].geometry_ts, pgs[i].geometry_ts, 2)
            self.assertEqual(polygons[i].tag, pgs[i].tag)
       
        # delete polygons
        t = time.time()
        self.client.delete_geometries(containers[0].uuid,
            geometry_type='Polygon')
        print 'delete polygons: %.2f' % (time.time() - t)
        
        # get polygons
        polygons = [e for e in self.client.get_geometries(containers[0].uuid,
            geometry_type='Polygon')]
        self.assertListEqual(polygons, [])
  
        # add polygons
        pgs = [snowfloat.geometry.Polygon(coordinates=[[[45.0, 45.0, 0],
                                                        [45.1, 45.0, 0],
                                                        [45.1, 45.1, 0],
                                                        [45.0, 45.0, 0]
                                                      ]],
                                          tag='test_tag_1',
                                          geometry_ts=1),
               snowfloat.geometry.Polygon(coordinates=[[[45.05, 45.05, 0],
                                                        [45.06, 45.0, 0],
                                                        [45.06, 45.06, 0],
                                                        [45.05, 45.05, 0]
                                                       ]],
                                          tag='test_tag_2',
                                          geometry_ts=2),
               snowfloat.geometry.Polygon(coordinates=[[[55.0, 55.0, 0],
                                                        [55.1, 55.0, 0],
                                                        [55.1, 55.1, 0],
                                                        [55.0, 55.0, 0]
                                                       ]],
                                          tag='test_tag_3',
                                          geometry_ts=3)]
        polygons = self.client.add_geometries(containers[0].uuid, pgs)
 
        # get polygons using a spatial lookup
        polygon = snowfloat.geometry.Polygon([[[45.0, 45.0, 0],
                                         [45.2, 45.0, 0],
                                         [45.2, 45.2, 0],
                                         [45.0, 45.0, 0]
                                       ]])
        polygons = [e for e in self.client.get_geometries(containers[0].uuid,
            geometry_type='Polygon', query='contained', geometry=polygon)]
        self.assertEqual(len(polygons), 2)
        self.assertListEqual(polygons[0].coordinates, [[[45.0, 45.0, 0],
                                                        [45.1, 45.0, 0],
                                                        [45.1, 45.1, 0],
                                                        [45.0, 45.0, 0]
                                                      ]])
        self.assertListEqual(polygons[1].coordinates, [[[45.05, 45.05, 0],
                                                        [45.06, 45.0, 0],
                                                        [45.06, 45.06, 0],
                                                        [45.05, 45.05, 0]
                                                      ]])

    def test_execute_tasks_distance(self):

        # add containers and points to compute on
        containers = [snowfloat.container.Container(tag='test_tag_1'),
                      snowfloat.container.Container(tag='test_tag_2')]
        containers = self.client.add_containers(containers)

        # add points
        for i in range(2):
            pts = [
                snowfloat.geometry.Point(
                coordinates=[
                 random.random() * 90,
                 random.random() * -90,
                 random.random() * 1000],
                tag='test_tag_%d' % (j+1),
                geometry_ts=time.time())
                for j in range(1000)]
            points = self.client.add_geometries(containers[i].uuid, pts)

        tasks = [snowfloat.task.Task(
                    operation='distance',
                    resource='points',
                    container_uuid=containers[0].uuid,
                    ts_range=(0, time.time())),
                 snowfloat.task.Task(
                    operation='distance',
                    resource='points',
                    container_uuid=containers[1].uuid,
                    ts_range=(0, time.time()))]
        t = time.time()
        r = self.client.execute_tasks(tasks)
        print 'distance: %.2f' % (time.time() - t)
        self.assertEqual(r[0][0]['count'], 1000)
        self.assertGreater(r[0][0]['distance'], 0)
        self.assertGreater(r[0][0]['velocity'], 0.)
        self.assertEqual(r[1][0]['count'], 1000)
        self.assertGreater(r[1][0]['distance'], 0)
        self.assertGreater(r[1][0]['velocity'], 0.)

    def test_execute_tasks_map(self):

        # add containers and points to draw on the map
        containers = [snowfloat.container.Container(tag='test_tag_1'),
                      snowfloat.container.Container(tag='test_tag_2')]
        containers = self.client.add_containers(containers)

        # add points
        for i in range(2):
            pts = [
                snowfloat.geometry.Point(
                coordinates=[
                 random.random() * 90,
                 random.random() * -90,
                 random.random() * 1000],
                tag='test_tag_%d' % (j+1),
                geometry_ts=time.time())
                for j in range(100)]
            points = self.client.add_geometries(containers[i].uuid, pts)
        
        tasks = [snowfloat.task.Task(
                    operation='map',
                    resource='points',
                    container_uuid=(containers[0].uuid, containers[1].uuid),
                    extras={'llcrnrlat': -75,
                            'llcrnrlon': -165,
                            'urcrnrlat': 75,
                            'urcrnrlon': 165},
                    ts_range=(0, time.time()))]
        t = time.time()
        r = self.client.execute_tasks(tasks)
        print 'map: %.2f' % (time.time() - t)
        self.assertTrue('url' in r[0][0])

    def test_execute_tasks_import_geospatial_data(self):

        tag_fields = ['dbl', 'int', 'str']
        path = 'tests/test_point.zip'
        r = self.client.import_geospatial_data(path, tag_fields)
        self.assertDictEqual(r,
            {'containers_count': 1, 'geometries_count': 5})

    def test_usa(self):

        # add container
        containers = [snowfloat.container.Container(tag='World')]
        containers = self.client.add_containers(containers)

        # add cities
        points = [snowfloat.geometry.Point(coordinates=[-122.41941550000001,
                                                        37.7749295,
                                                        0],
                                           tag='San Francisco',
                                           geometry_ts=1),
                  snowfloat.geometry.Point(coordinates=[-104.98471790000002,
                                                        39.737567,
                                                        0],
                                           tag='Denver',
                                           geometry_ts=2),
                  snowfloat.geometry.Point(coordinates=[-71.0597732,
                                                        42.3584308,
                                                        0],
                                           tag='Boston',
                                           geometry_ts=3),
                  snowfloat.geometry.Point(coordinates=[2.3522219000000177,
                                                        48.856614,
                                                        0],
                                           tag='Paris',
                                           geometry_ts=4),
                 ]
        points = self.client.add_geometries(containers[0].uuid, points)

        # add two polygons: west and east
        poly_west = snowfloat.geometry.Polygon(
                        coordinates=[[[-125, 40, 0],
                                      [-125, 30, 0],
                                      [-100, 30, 0],
                                      [-100, 40, 0],
                                      [-125, 40, 0]]], tag='West')
        poly_east = snowfloat.geometry.Polygon(
                        coordinates=[[[-75, 45, 0],
                                      [-75, 35, 0],
                                      [-65, 35, 0],
                                      [-65, 45, 0],
                                      [-75, 45, 0]]], tag='East')
        
        polygons = self.client.add_geometries(containers[0].uuid,
            [poly_west, poly_east])

        # look for points contained by the west polygon
        pts = [e for e in self.client.get_geometries(containers[0].uuid,
            geometry_type='Point', query='contained', geometry=poly_west)]
        self.assertEqual(len(pts), 2)
        self.assertListEqual(pts[0].coordinates,
            [-122.4194155, 37.7749295, 0])
        self.assertListEqual(pts[1].coordinates,
            [-104.9847179, 39.737567, 0])

        # look for points using distance_lt
        pl = snowfloat.geometry.Point(coordinates=[2.45, 48.85, 0])
        pts = [e for e in self.client.get_geometries(containers[0].uuid,
            geometry_type='Point', query='distance_lt', geometry=pl, distance=20000)]
        self.assertEqual(len(pts), 1)
        self.assertListEqual(pts[0].coordinates,
            [2.3522219, 48.856614, 0.0])

        # spatial operation distance
        pl = snowfloat.geometry.Point(coordinates=[2.45, 48.85, 0])
        pts = [e for e in self.client.get_geometries(containers[0].uuid,
            geometry_type='Point', spatial_operation='distance', spatial_geometry=pl)]
        self.assertEqual(len(pts), 4)
        self.assertListEqual([p.spatial for p in pts],
            [8958661.49177521, 7866626.32482402, 5538314.71584491,
             7191.65275350335])

        # task distance
        tasks = [snowfloat.task.Task(
                    operation='distance',
                    resource='points',
                    container_uuid=containers[0].uuid,
                    ts_range=(0, time.time()))]
        r = self.client.execute_tasks(tasks)
        self.assertListEqual(r,
            [[{u'count': 4, u'distance': 9722488.93059933,
               u'velocity': 3240829.64353311}]])

        # task map
        tasks = [snowfloat.task.Task(
                    operation='map',
                    resource='points',
                    container_uuid=containers[0].uuid,
                    extras={'llcrnrlat': -75,
                            'llcrnrlon': -165,
                            'urcrnrlat': 75,
                            'urcrnrlon': 165},
                    ts_range=(0, time.time()))]
        r = self.client.execute_tasks(tasks)
        self.assertTrue('url' in r[0][0])


if __name__ == "__main__":
    unittest.main()
