"""Configuration constants for the DC Bus Delay Tracker."""

# "On-time" definition: between 2 minutes early and 5 minutes late
ON_TIME_MIN = -2.0
ON_TIME_MAX = 5.0

# S3 key prefixes
DATA_PREFIX = 'data/'

# Ward GeoJSON bundled with Lambda
WARD_GEOJSON_FILENAME = 'dc-wards.geojson'

# Route metadata refresh interval (days)
METADATA_REFRESH_DAYS = 7
