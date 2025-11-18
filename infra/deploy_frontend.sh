#!/bin/bash
# Deploy frontend to S3 bucket
# Usage: ./deploy_frontend.sh

set -e

echo "=== Movie Engine Frontend Deployment ==="
echo ""

# Get API URL from CloudFormation stack
echo "Fetching API URL from CloudFormation..."
API_URL=$(aws cloudformation describe-stacks \
    --stack-name MovieEngineAPIStack \
    --query "Stacks[0].Outputs[?ExportName=='MovieEngineApiUrl'].OutputValue" \
    --output text 2>/dev/null)

if [ -z "$API_URL" ]; then
    echo "Warning: Could not fetch API URL from CloudFormation."
    echo "Using default: http://localhost:8000"
    API_URL="http://localhost:8000"
else
    echo "API URL: $API_URL"
fi
echo ""

# Build the frontend with API URL
echo "Building frontend..."
cd ../movie-engine-fe

# Create .env file with API URL
echo "VITE_API_URL=$API_URL" > .env

npm run build

# Check if dist directory exists
if [ ! -d "dist" ]; then
    echo "Error: dist directory not found. Build may have failed."
    exit 1
fi

echo "Build complete!"
echo ""

# Deploy to S3
echo "Deploying to S3..."
BUCKET_NAME="movie-engine-frontend"

# Sync files to S3
aws s3 sync dist/ s3://${BUCKET_NAME}/ \
    --delete \
    --cache-control "public, max-age=31536000" \
    --exclude "index.html"

# Upload index.html with no cache (for SPA updates)
aws s3 cp dist/index.html s3://${BUCKET_NAME}/index.html \
    --cache-control "no-cache, no-store, must-revalidate"

echo ""
echo "Deployment complete!"
echo ""
echo "Frontend URL: http://${BUCKET_NAME}.s3-website-us-east-1.amazonaws.com"
echo "API URL: $API_URL"
echo ""
