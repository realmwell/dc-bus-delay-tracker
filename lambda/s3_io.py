"""S3 I/O helpers for reading/writing aggregated data files."""

import boto3
import json
import logging

logger = logging.getLogger(__name__)


class S3IO:
    def __init__(self, bucket):
        self.s3 = boto3.client('s3')
        self.bucket = bucket

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

    def copy_json(self, src_key, dest_key):
        """Copy a JSON file within the same bucket."""
        try:
            self.s3.copy_object(
                Bucket=self.bucket,
                CopySource={'Bucket': self.bucket, 'Key': src_key},
                Key=dest_key,
                ContentType='application/json',
                CacheControl='public, max-age=3600',
                MetadataDirective='REPLACE',
            )
        except Exception as e:
            logger.warning(f'Error copying {src_key} to {dest_key}: {e}')
