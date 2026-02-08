"""Pure Python point-in-polygon using ray casting. No external dependencies."""

import json
import os


def ray_cast_contains(point_lon, point_lat, polygon_coords):
    """Check if point (lon, lat) is inside a polygon ring.

    Uses the ray casting algorithm. polygon_coords is a list of [lon, lat] pairs.
    """
    n = len(polygon_coords)
    inside = False
    x, y = point_lon, point_lat
    j = n - 1

    for i in range(n):
        xi, yi = polygon_coords[i]
        xj, yj = polygon_coords[j]

        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i

    return inside


def point_in_ward(lat, lon, ward_polygons):
    """Determine which DC ward a lat/lon point falls in.

    Args:
        lat: Latitude
        lon: Longitude
        ward_polygons: dict of {ward_number_str: list_of_polygon_rings}

    Returns:
        Ward number (1-8) or None if outside all wards.
    """
    for ward_num, polygon_rings in ward_polygons.items():
        # GeoJSON polygons: first ring is exterior, rest are holes
        exterior = polygon_rings[0]
        if ray_cast_contains(lon, lat, exterior):
            in_hole = False
            for hole in polygon_rings[1:]:
                if ray_cast_contains(lon, lat, hole):
                    in_hole = True
                    break
            if not in_hole:
                return int(ward_num)
    return None


def load_ward_polygons(geojson_path=None):
    """Load DC ward boundary GeoJSON and return dict of ward -> polygon coords.

    Returns:
        dict: {ward_number_str: [exterior_ring, hole_ring, ...]}
        Each ring is a list of [lon, lat] coordinate pairs.
    """
    if geojson_path is None:
        geojson_path = os.path.join(os.path.dirname(__file__), 'dc-wards.geojson')

    with open(geojson_path, 'r') as f:
        geojson = json.load(f)

    ward_polygons = {}
    for feature in geojson['features']:
        ward_num = str(feature['properties']['WARD'])
        geom = feature['geometry']

        if geom['type'] == 'Polygon':
            ward_polygons[ward_num] = geom['coordinates']
        elif geom['type'] == 'MultiPolygon':
            # Flatten: treat each sub-polygon's rings together
            all_rings = []
            for polygon in geom['coordinates']:
                all_rings.extend(polygon)
            ward_polygons[ward_num] = all_rings

    return ward_polygons
