"""Geometry tests."""


import tests.helper

import snowfloat.geometry
import snowfloat.task

class PolygonsTests(tests.helper.Tests):
    """Polygon tests."""
    def test_polygon_not_closed(self):
        """Test polygon closing feature."""
        polygon = snowfloat.geometry.Polygon(
            [[[0, 0, 0], [1, 1, 0], [1, 0, 0]]])
        self.assertListEqual(polygon.coordinates,
            [[[0, 0, 0], [1, 1, 0], [1, 0, 0], [0, 0, 0]]])


class GeometriesTests(tests.helper.Tests):
    """Geometries tests."""
    def test_geometry(self):
        """Parent Geometry test."""
        geometry = snowfloat.geometry.Geometry([1, 2, 3])
        self.assertListEqual(geometry.coordinates, [1, 2, 3])
        self.assertIsNone(geometry.geometry_type)
        self.assertRaises(NotImplementedError,
            geometry.num_points)
