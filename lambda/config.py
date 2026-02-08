"""Configuration constants for the DC Bus Delay Tracker."""

TIME_PERIODS = {
    '1d': 1,
    '1w': 7,
    '1m': 30,
    '3m': 90,
    '6m': 180,
    '1y': 365,
    '5y': 1825,
}

# "On-time" definition: between 2 minutes early and 5 minutes late
ON_TIME_MIN = -2.0
ON_TIME_MAX = 5.0

# S3 key prefixes
RAW_PREFIX = 'raw/'
DATA_PREFIX = 'data/'

# Ward GeoJSON bundled with Lambda
WARD_GEOJSON_FILENAME = 'dc-wards.geojson'

# Route metadata refresh interval (days)
METADATA_REFRESH_DAYS = 7
