"""Geometries objects: Points, Polygons."""

import json
import sys
import time

try:
    import shapely.geometry
    POINT_CLS = shapely.geometry.Point
    POLYGON_CLS = shapely.geometry.Polygon
except ImportError:
    POINT_CLS = object
    POLYGON_CLS = object

class Geometry(object):
    """Parent class of all geometries.

    Attributes:
        coordinates (list): Geometry coordinates. 
        
        geometry_type(str): Point or Polygon.

    """

    coordinates = None
    geometry_type = None

    def __init__(self, coordinates, **kwargs):
        for key, val in kwargs.items():
            getattr(self, key)
            setattr(self, key, val)
        self.coordinates = coordinates
    
    def __str__(self):
        return '%s(coordinates=%s)' \
            % (self.__class__.__name__, self.coordinates)

    def num_points(self):
        """Returns the geometry number of points."""
        raise NotImplementedError()


class Point(Geometry, POINT_CLS):
    """Geometry Point.
    """

    geometry_type = 'Point'

    def __init__(self, coordinates, **kwargs):
        coords = coordinates
        if POINT_CLS != object:
            shapely.geometry.Point.__init__(self, coords)
        if len(coords) == 2:
            coords.append(0)
        if coords[2] == None:
            coords[2] = 0
        Geometry.__init__(self, coords, **kwargs)
    
    def num_points(self):
        """Geometry Point has one point."""
        return 1


class Polygon(Geometry, POLYGON_CLS):
    """Geometry Polygon."""

    geometry_type = 'Polygon'

    def __init__(self, coordinates, **kwargs):
        coords = coordinates
        if POLYGON_CLS != object:
            shapely.geometry.Polygon.__init__(self, coords[0])
        for coordinates in coords[0]:
            if len(coordinates) == 3 and coordinates[2] == None:
                coordinates[2] = 0
        if coords[0][0] != coords[0][-1]:
            coords[0].append(coords[0][0])
        Geometry.__init__(self, coords, **kwargs)

    def num_points(self):
        """Returns the number of points defining this polygon."""
        return len(self.coordinates[0])


