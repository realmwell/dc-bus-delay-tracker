"""S3 I/O helpers for reading/writing raw archives and aggregated data."""

import boto3
import json
import gzip
import io
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


class S3IO:
    def __init__(self, bucket):
        self.s3 = boto3.client('s3')
        self.bucket = bucket

    def save_daily_snapshot(self, date_str, positions, total_bus_count):
        """Save compressed daily snapshot to raw/YYYY/MM/DD.json.gz."""
        year, month, day = date_str.split('-')
        key = f'raw/{year}/{month}/{day}.json.gz'

        payload = {
            'date': date_str,
            'captured_at': datetime.now(timezone.utc).isoformat(),
            'bus_count': total_bus_count,
            'positions': positions,
        }

        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode='wb') as gz:
            gz.write(json.dumps(payload, separators=(',', ':')).encode('utf-8'))

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType='application/gzip',
        )
        logger.info(f'Saved snapshot: {key} ({len(positions)} DC positions, {total_bus_count} total)')

    def load_historical_snapshots(self, today_str, max_days=1825):
        """Load daily snapshots for the last max_days days.

        Returns list of (date_str, positions_list) sorted newest first.
        """
        today = datetime.strptime(today_str, '%Y-%m-%d')
        results = []

        for days_ago in range(max_days):
            date = today - timedelta(days=days_ago)
            key = f'raw/{date:%Y}/{date:%m}/{date:%d}.json.gz'

            try:
                obj = self.s3.get_object(Bucket=self.bucket, Key=key)
                with gzip.GzipFile(fileobj=io.BytesIO(obj['Body'].read())) as gz:
                    data = json.loads(gz.read().decode('utf-8'))
                results.append((date.strftime('%Y-%m-%d'), data['positions']))
            except self.s3.exceptions.NoSuchKey:
                continue
            except Exception as e:
                logger.warning(f'Error loading {key}: {e}')
                continue

        logger.info(f'Loaded {len(results)} historical snapshots')
        return results

    def write_json(self, key, data):
        """Write JSON file to S3 with cache headers."""
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(data, separators=(',', ':')).encode('utf-8'),
            ContentType='application/json',
            CacheControl='public, max-age=3600',
        )

    def read_json(self, key):
        """Read JSON file from S3. Returns None if not found."""
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            return json.loads(obj['Body'].read().decode('utf-8'))
        except Exception:
            return None

    def write_last_updated(self, today, total_count, dc_count, history_count):
        """Write status file."""
        self.write_json('data/last-updated.json', {
            'last_run': datetime.now(timezone.utc).isoformat(),
            'status': 'success',
            'date': today,
            'bus_positions_fetched': total_count,
            'dc_positions': dc_count,
            'days_of_history': history_count,
        })
