#!/usr/bin/env python3
"""Import WMATA Service Excellence historical bus OTP data into our S3 format.

This reads the WMATA Excel file and creates:
1. Monthly raw snapshots in /raw/ (synthetic, system-wide)
2. Re-runs aggregation for all time periods

The historical data is system-wide (not per-ward), so we distribute it
evenly across all 8 wards. Ward-level granularity begins when our daily
Lambda starts collecting per-bus GPS data.

Usage:
    python3 scripts/import_historical.py
"""

import json
import gzip
import io
import os
import sys
import calendar
from datetime import datetime, timezone

# Add lambda dir to path for shared modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambda'))
from config import TIME_PERIODS, ON_TIME_MIN, ON_TIME_MAX

try:
    import openpyxl
except ImportError:
    print("Installing openpyxl...")
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'openpyxl', '-q'])
    import openpyxl

try:
    import boto3
except ImportError:
    print("Installing boto3...")
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'boto3', '-q'])
    import boto3

BUCKET = None
EXCEL_PATH = os.path.join(os.path.dirname(__file__), '..', 'data-import', 'wmata-service-excellence.xlsx')

MONTH_MAP = {
    'January': 1, 'February': 2, 'March': 3, 'April': 4,
    'May': 5, 'June': 6, 'July': 7, 'August': 8,
    'September': 9, 'October': 10, 'November': 11, 'December': 12,
}


def get_bucket_name():
    """Get bucket name from CloudFormation stack outputs."""
    cf = boto3.client('cloudformation')
    resp = cf.describe_stacks(StackName='dc-bus-tracker')
    for output in resp['Stacks'][0]['Outputs']:
        if output['OutputKey'] == 'BucketName':
            return output['OutputValue']
    raise RuntimeError("Could not find BucketName in stack outputs")


def read_excel():
    """Read BUOTP sheet from WMATA Excel file. Returns list of monthly records."""
    wb = openpyxl.load_workbook(EXCEL_PATH, read_only=True)
    ws = wb['BUOTP']

    records = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        year, month_name = row[0], row[1]
        if year is None or month_name is None:
            continue

        pct_early = row[6]
        pct_late = row[7]
        pct_on_time = row[8]

        if pct_on_time is None or pct_on_time == 'no data':
            continue

        month_num = MONTH_MAP.get(month_name)
        if month_num is None:
            continue

        total_timepoints = row[5] or 0

        records.append({
            'year': int(year),
            'month': month_num,
            'pct_early': float(pct_early) * 100,
            'pct_late': float(pct_late) * 100,
            'pct_on_time': float(pct_on_time) * 100,
            'total_timepoints': int(total_timepoints),
        })

    records.sort(key=lambda r: (r['year'], r['month']))
    print(f"Read {len(records)} monthly records from Excel")
    print(f"  Range: {records[0]['year']}-{records[0]['month']:02d} to {records[-1]['year']}-{records[-1]['month']:02d}")
    return records


def estimate_avg_delay(pct_late, pct_early):
    """Estimate average delay in minutes from percentages.

    Based on WMATA's definition: on-time = -2 to +7 min.
    We estimate:
    - Average on-time bus: ~1 min late
    - Average late bus: ~10 min late
    - Average early bus: ~3 min early
    These are rough estimates that produce realistic avg_delay values.
    """
    pct_on_time = 100.0 - pct_late - pct_early
    avg = (
        (pct_on_time / 100) * 1.0 +    # on-time buses avg ~1 min
        (pct_late / 100) * 10.0 +        # late buses avg ~10 min
        (pct_early / 100) * (-3.0)       # early buses avg ~3 min early
    )
    return round(avg, 1)


def create_synthetic_snapshots(records, s3):
    """Create one synthetic raw snapshot per month.

    We create a snapshot on the 15th of each month with synthetic
    bus positions distributed across all 8 wards. The deviation values
    are generated to match the reported percentages.
    """
    for rec in records:
        year = rec['year']
        month = rec['month']
        date_str = f"{year}-{month:02d}-15"

        # Create synthetic positions that reproduce the percentages
        # We'll create ~200 positions (similar to our real daily snapshots)
        # distributed across 8 wards
        positions = []
        buses_per_ward = 25

        for ward in range(1, 9):
            for i in range(buses_per_ward):
                # Assign deviations to match system-wide percentages
                frac = i / buses_per_ward
                if frac < rec['pct_early'] / 100:
                    dev = -3.0  # early
                elif frac < (rec['pct_early'] + rec['pct_on_time']) / 100:
                    dev = 1.0   # on-time
                else:
                    dev = 10.0  # late

                positions.append({
                    'vid': f'HIST-{ward}-{i}',
                    'route': f'HIST-{ward}',
                    'dev': dev,
                    'ward': ward,
                    'lat': 38.9,
                    'lon': -77.0,
                    'dt': f'{date_str}T12:00:00Z',
                    'trip': f'HIST-{ward}-{i}',
                })

        # Save as compressed snapshot
        key = f'raw/{year}/{month:02d}/15.json.gz'
        payload = {
            'date': date_str,
            'captured_at': f'{date_str}T12:00:00Z',
            'bus_count': len(positions),
            'source': 'wmata-historical-import',
            'positions': positions,
        }

        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode='wb') as gz:
            gz.write(json.dumps(payload, separators=(',', ':')).encode('utf-8'))

        s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue(), ContentType='application/gzip')

    print(f"Created {len(records)} synthetic monthly snapshots in S3")


def main():
    global BUCKET
    BUCKET = get_bucket_name()
    print(f"Bucket: {BUCKET}")

    s3 = boto3.client('s3')
    records = read_excel()

    # Check which snapshots already exist (don't overwrite real data)
    existing = set()
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix='raw/'):
        for obj in page.get('Contents', []):
            existing.add(obj['Key'])

    # Filter out months where we already have real data
    new_records = []
    for rec in records:
        key = f"raw/{rec['year']}/{rec['month']:02d}/15.json.gz"
        if key not in existing:
            new_records.append(rec)
        else:
            print(f"  Skipping {rec['year']}-{rec['month']:02d} (already has data)")

    if not new_records:
        print("No new historical records to import.")
    else:
        print(f"Importing {len(new_records)} new monthly snapshots...")
        create_synthetic_snapshots(new_records, s3)

    # Now trigger Lambda to re-aggregate with the new historical data
    print("\nInvoking Lambda to re-aggregate all time periods...")
    lam = boto3.client('lambda')
    resp = lam.invoke(
        FunctionName='dc-bus-tracker-collector',
        InvocationType='RequestResponse',
    )
    payload = json.loads(resp['Payload'].read())
    print(f"Lambda response: {payload}")

    print("\nDone! Historical data imported and aggregated.")
    print("Invalidate CloudFront cache with:")
    print("  aws cloudfront create-invalidation --distribution-id E31GFJS78VYFLV --paths '/data/*'")


if __name__ == '__main__':
    main()
