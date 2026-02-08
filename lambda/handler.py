"""Lambda handler for the DC Bus Delay Tracker.

Triggered daily by EventBridge. Fetches WMATA bus positions,
maps them to DC wards, and writes pre-aggregated JSON files
to S3 for the static frontend. No raw data is stored — each
run fully replaces all output files.
"""

import os
import logging
from datetime import datetime, timezone

from wmata_client import WMATAClient
from geo_utils import point_in_ward
from aggregator import build_1d_views, build_historical_views
from stop_ward_mapper import StopWardMapper
from s3_io import S3IO

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    wmata = WMATAClient(api_key=os.environ['WMATA_API_KEY'])
    s3 = S3IO(bucket=os.environ['BUCKET_NAME'])
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # 1. Fetch all bus positions
    logger.info('Fetching bus positions from WMATA...')
    positions = wmata.get_bus_positions()
    logger.info(f'Fetched {len(positions)} bus positions')

    # 2. Load ward boundaries
    mapper = StopWardMapper(s3=s3, wmata=wmata)
    ward_polygons = mapper.get_ward_polygons()

    # 3. Map each bus position to a ward
    enriched = []
    for pos in positions:
        if pos.get('Deviation') is None:
            continue
        ward = point_in_ward(pos['Lat'], pos['Lon'], ward_polygons)
        if ward is not None:
            enriched.append({
                'vid': pos.get('VehicleID', ''),
                'route': pos.get('RouteID', ''),
                'dev': pos['Deviation'],
                'ward': ward,
                'lat': pos['Lat'],
                'lon': pos['Lon'],
                'dt': pos.get('DateTime', ''),
                'trip': pos.get('TripID', ''),
            })

    logger.info(f'Mapped {len(enriched)} positions to DC wards')

    # 4. Refresh route metadata if needed
    route_meta = mapper.ensure_route_metadata()

    # 5. Build 1D views (per-ward from live data)
    build_1d_views(enriched, route_meta, s3)

    # 6. Build historical period views (from WMATA monthly report data)
    build_historical_views(s3)

    # 7. Write status file
    s3.write_json('data/last-updated.json', {
        'last_run': datetime.now(timezone.utc).isoformat(),
        'status': 'success',
        'date': today,
        'bus_positions_fetched': len(positions),
        'dc_positions': len(enriched),
    })

    msg = f'Processed {len(enriched)} DC bus positions for {today}'
    logger.info(msg)
    return {'statusCode': 200, 'body': msg}
