"""Geometries objects: Points, Polygons."""

try:
    import shapely.geometry
    POINT_CLS = shapely.geometry.Point
    LINESTRING_CLS = shapely.geometry.LineString
    POLYGON_CLS = shapely.geometry.Polygon
    MULTIPOINT_CLS = shapely.geometry.MultiPoint
    MULTIPOLYGON_CLS = shapely.geometry.MultiPolygon
    MULTILINESTRING_CLS = shapely.geometry.MultiLineString
except ImportError:
    POINT_CLS = object
    LINESTRING_CLS = object
    POLYGON_CLS = object
    MULTIPOINT_CLS = object
    MULTIPOLYGON_CLS = object
    MULTILINESTRING_CLS = object

class Geometry(object):
    """Parent class of all geometries.

    Attributes:
        coordinates (list): Geometry coordinates. 
        
        geometry_type (str): Point, LineString... 

    """

    coordinates = None
    geometry_type = None

    def __init__(self, coordinates):
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

    def __init__(self, coordinates):
        coords = coordinates[:]
        if POINT_CLS != object:
            shapely.geometry.Point.__init__(self, coords)
        Geometry.__init__(self, coords)
    
    def num_points(self):
        """Geometry Point has one point."""
        return 1


class LineString(Geometry, LINESTRING_CLS):
    """Geometry LineString.
    """

    geometry_type = 'LineString'
    points = None

    def __init__(self, coordinates):
        coords = coordinates[:]
        self.points = [Point(c) for c in coords] 
        if LINESTRING_CLS != object:
            shapely.geometry.LineString.__init__(self, coords)
        Geometry.__init__(self, coords)
    
    def num_points(self):
        """Returns the number of points defining this linestring."""
        return len(self.coordinates)


class Polygon(Geometry, POLYGON_CLS):
    """Geometry Polygon."""

    geometry_type = 'Polygon'

    def __init__(self, coordinates):
        coords = coordinates[:]
        if POLYGON_CLS != object:
            shapely.geometry.Polygon.__init__(self, coords[0], coords[1:])
        # close the rings if not closed
        for coord in coords:
            if coord[0] != coord[-1]:
                coord.append(coord[0])
        Geometry.__init__(self, coords)

    def num_points(self):
        """Returns the number of points defining this polygon."""
        return len(self.coordinates[0])


class MultiPoint(Geometry, MULTIPOINT_CLS):
    """Geometry MultiPoint."""

    geometry_type = 'MultiPoint'
    points = None

    def __init__(self, coordinates):
        coords = coordinates[:]
        self.points = [Point(c) for c in coords] 
        if MULTIPOINT_CLS != object:
            shapely.geometry.MultiPoint.__init__(self, coords)
        Geometry.__init__(self, coords)
    
    def num_points(self):
        """Returns the number of points defining this multipoint."""
        return len(self.coordinates)


class MultiPolygon(Geometry, MULTIPOLYGON_CLS):
    """Geometry MultiPolygon."""

    geometry_type = 'MultiPolygon'
    polygons = None

    def __init__(self, coordinates):
        coords = coordinates[:]
        self.polygons = [Polygon(c) for c in coords] 
        if MULTIPOLYGON_CLS != object:
            shapely.geometry.MultiPolygon.__init__(self, self.polygons)
        Geometry.__init__(self, coords)

    def num_points(self):
        """Returns the number of points defining this multipolygon."""
        return sum([p.num_points() for p in self.polygons])


class MultiLineString(Geometry, MULTILINESTRING_CLS):
    """Geometry MultiLineString."""

    geometry_type = 'MultiLineString'
    linestrings = None

    def __init__(self, coordinates):
        coords = coordinates[:]
        self.linestrings = [LineString(c) for c in coords] 
        if MULTILINESTRING_CLS != object:
            shapely.geometry.MultiLineString.__init__(self, self.linestrings)
        Geometry.__init__(self, coords)

    def num_points(self):
        """Returns the number of points defining this multilinestrings."""
        return sum([e.num_points() for e in self.linestrings])


class GeometryCollection(Geometry):
    """Geometry Collection."""

    geometry_type = 'GeometryCollection'
    geometries = None

    def __init__(self, geometries):
        self.geometries = geometries
        Geometry.__init__(self, None)

    def num_points(self):
        """Returns the number of points defining this collection."""
        return sum([e.num_points() for e in self.geometries])
