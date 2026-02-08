"""WMATA API client. Fetches bus positions, stops, and routes."""

import urllib.request
import json
import time
import logging

logger = logging.getLogger(__name__)

BASE_URL = 'https://api.wmata.com'


class WMATAClient:
    def __init__(self, api_key):
        self.api_key = api_key

    def _get(self, path):
        """Make authenticated GET request to WMATA API with retries."""
        url = f'{BASE_URL}{path}'
        req = urllib.request.Request(url)
        req.add_header('api_key', self.api_key)

        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return json.loads(resp.read().decode('utf-8'))
            except Exception as e:
                logger.warning(f'WMATA API attempt {attempt + 1} failed for {path}: {e}')
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    raise

    def get_bus_positions(self):
        """Fetch all current bus positions with deviation data.

        Returns list of dicts with keys:
            VehicleID, RouteID, Deviation, Lat, Lon, DateTime, TripID, DirectionText
        """
        data = self._get('/Bus.svc/json/jBusPositions')
        return data.get('BusPositions', [])

    def get_stops(self):
        """Fetch all bus stops.

        Returns list of dicts with keys:
            StopID, Name, Lat, Lon, Routes
        """
        data = self._get('/Bus.svc/json/jStops')
        return data.get('Stops', [])

    def get_routes(self):
        """Fetch all bus routes.

        Returns list of dicts with keys:
            RouteID, Name, LineDescription
        """
        data = self._get('/Bus.svc/json/jRoutes')
        return data.get('Routes', [])
