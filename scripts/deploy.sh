#!/bin/bash
set -euo pipefail

# DC Bus Delay Tracker - Build and Deploy
# Usage: ./scripts/deploy.sh [WMATA_API_KEY] [ALERT_EMAIL]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
STACK_NAME="dc-bus-tracker"

cd "$PROJECT_DIR"

echo "=== Building Lambda package ==="
sam build --template template.yaml

echo ""
echo "=== Deploying CloudFormation stack ==="

if [ $# -ge 2 ]; then
    sam deploy \
        --stack-name "$STACK_NAME" \
        --capabilities CAPABILITY_IAM \
        --parameter-overrides \
            "WMATAApiKey=$1" \
            "AlertEmail=$2" \
        --no-confirm-changeset \
        --no-fail-on-empty-changeset
else
    echo "No parameters provided. Running guided deployment..."
    sam deploy \
        --guided \
        --stack-name "$STACK_NAME" \
        --capabilities CAPABILITY_IAM
fi

echo ""
echo "=== Stack Outputs ==="
aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs' \
    --output table

echo ""
echo "=== Next Steps ==="
echo "1. Run: ./scripts/upload-site.sh"
echo "2. Confirm the billing alarm email subscription"
echo "3. Manually invoke Lambda for first data: aws lambda invoke --function-name dc-bus-tracker-collector /dev/stdout"
