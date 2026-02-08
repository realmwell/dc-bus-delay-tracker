"""Builds and caches the mapping from WMATA bus stops to DC wards."""

import logging
from datetime import datetime, timezone, timedelta

from geo_utils import load_ward_polygons, point_in_ward
from config import METADATA_REFRESH_DAYS

logger = logging.getLogger(__name__)


class StopWardMapper:
    def __init__(self, s3, wmata):
        self.s3 = s3
        self.wmata = wmata
        self._ward_polygons = None

    def get_ward_polygons(self):
        """Load ward polygon boundaries from bundled GeoJSON."""
        if self._ward_polygons is None:
            self._ward_polygons = load_ward_polygons()
        return self._ward_polygons

    def ensure_route_metadata(self):
        """Load route metadata from S3 cache, or rebuild if stale.

        Returns dict of route_id -> {name, line, wards}.
        """
        existing = self.s3.read_json('data/route-metadata.json')
        if existing:
            try:
                gen_time = datetime.fromisoformat(existing['generated_at'])
                if datetime.now(timezone.utc) - gen_time < timedelta(days=METADATA_REFRESH_DAYS):
                    return existing.get('routes', {})
            except (KeyError, ValueError):
                pass

        return self._rebuild_metadata()

    def _rebuild_metadata(self):
        """Fetch routes and stops from WMATA, build ward mappings."""
        logger.info('Rebuilding route metadata from WMATA API...')

        routes_raw = self.wmata.get_routes()
        stops_raw = self.wmata.get_stops()
        ward_polys = self.get_ward_polygons()

        # Map each stop to a ward
        stop_wards = {}
        for stop in stops_raw:
            ward = point_in_ward(stop['Lat'], stop['Lon'], ward_polys)
            if ward is not None:
                stop_wards[stop['StopID']] = {
                    'ward': ward,
                    'name': stop['Name'],
                    'routes': stop.get('Routes', []),
                }

        # Build route -> set of wards from stop data
        route_ward_sets = {}
        for stop_info in stop_wards.values():
            for route_id in stop_info['routes']:
                route_ward_sets.setdefault(route_id, set()).add(stop_info['ward'])

        # Build final route metadata
        route_meta = {}
        for route in routes_raw:
            rid = route['RouteID']
            route_meta[rid] = {
                'name': route.get('Name', rid),
                'line': route.get('LineDescription', ''),
                'wards': sorted(list(route_ward_sets.get(rid, []))),
            }

        now_iso = datetime.now(timezone.utc).isoformat()

        self.s3.write_json('data/route-metadata.json', {
            'generated_at': now_iso,
            'routes': route_meta,
        })

        self.s3.write_json('data/stop-ward-map.json', {
            'generated_at': now_iso,
            'stop_count': len(stops_raw),
            'dc_stop_count': len(stop_wards),
            'mapping': stop_wards,
        })

        logger.info(f'Route metadata rebuilt: {len(route_meta)} routes, {len(stop_wards)} DC stops')
        return route_meta
