#!/bin/bash
set -euo pipefail

# Upload static site files to S3 and invalidate CloudFront cache
# Usage: ./scripts/upload-site.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
STACK_NAME="dc-bus-tracker"

# Get stack outputs
BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`BucketName`].OutputValue' \
    --output text)

DIST_ID=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`DistributionId`].OutputValue' \
    --output text)

echo "Bucket: $BUCKET"
echo "Distribution: $DIST_ID"

echo ""
echo "=== Uploading site files to S3 ==="
aws s3 sync "$PROJECT_DIR/site/" "s3://$BUCKET/site/" \
    --delete \
    --cache-control "public, max-age=86400"

# Also copy ward GeoJSON to data prefix (frontend fetches from /data/)
echo ""
echo "=== Uploading ward GeoJSON to data prefix ==="
aws s3 cp "$PROJECT_DIR/site/data/dc-wards.geojson" "s3://$BUCKET/data/dc-wards.geojson" \
    --content-type "application/json" \
    --cache-control "public, max-age=604800"

echo ""
echo "=== Invalidating CloudFront cache ==="
aws cloudfront create-invalidation \
    --distribution-id "$DIST_ID" \
    --paths "/*" \
    --output text

echo ""
SITE_URL=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`WebsiteURL`].OutputValue' \
    --output text)

echo "=== Done ==="
echo "Site: $SITE_URL"
