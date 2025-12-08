#!/bin/bash
# Full deployment of Movie Engine (API + Frontend)
# Usage: ./deploy_all.sh

set -e

echo "========================================="
echo "   Movie Engine - Full Deployment"
echo "========================================="
echo ""

# Check for TMDB API key
if [ -z "$TMDB_API_KEY" ]; then
    echo "⚠️  TMDB_API_KEY not set."
    echo "Please enter your TMDB API key (or press Enter to skip):"
    read -s tmdb_key
    if [ -n "$tmdb_key" ]; then
        export TMDB_API_KEY="$tmdb_key"
    fi
    echo ""
fi

# Check for Enable Warming and Enable Provisioned Concurrency environment variables
# and exit with an error that tells the user that both cannot be set at the same time
if [ -n "$ENABLE_WARMING" ] && [ -n "$ENABLE_PROVISIONED_CONCURRENCY" ]; then
    echo "❌ Error: ENABLE_WARMING and ENABLE_PROVISIONED_CONCURRENCY cannot both be set at the same time."
    echo "Please choose one method for reducing cold starts."
    exit 1
fi

# Check for Scheduled Warming environment variable
# If set, tell the user that scheduled warming will be used for the lambda
if [ -n "$ENABLE_WARMING" ]; then
    echo "ℹ️  Scheduled Warming is enabled for the Lambda functions."
    echo "The lambda will be invoked every ${WARMING_RATE_MINUTES:-5} minutes to keep it warm."
    echo "This will help reduce cold start latency."
    echo ""
fi

# Check for Provisioned Concurrency environment variable
# If set, tell the user that provisioned concurrency will be used for the lambda
if [ -n "$ENABLE_PROVISIONED_CONCURRENCY" ]; then
    echo "ℹ️  Provisioned Concurrency is enabled for the Lambda functions."
    echo "This will help reduce cold start latency by keeping ${PROVISIONED_CONCURRENCY_INSTANCES:-1} instance(s) warm."
    echo ""
fi

# If neither is set, warn the user that cold starts may occur and ask for confirmation to proceed
if [ -z "$ENABLE_WARMING" ] && [ -z "$ENABLE_PROVISIONED_CONCURRENCY" ]; then
    echo "⚠️  Neither ENABLE_WARMING nor ENABLE_PROVISIONED_CONCURRENCY is set."
    echo "Cold starts may occur, leading to higher latency for the first requests."
    echo "Do you want to proceed? (y/n)"
    read proceed
    if [ "$proceed" != "y" ]; then
        echo "Deployment aborted."
        exit 1
    fi
    echo ""
fi

# Step 1: Deploy Backend
echo "📦 Step 1/4: Deploying Backend API..."
echo "========================================="
cdk deploy MovieEngineAPIStack --require-approval never
echo ""

# Step 2: Upload Model Files (if not already uploaded)
echo "📊 Step 2/4: Checking Model Files..."
echo "========================================="

# Check if bucket exists and has model files
if aws s3 ls s3://movie-engine-data/models/ 2>&1 > /dev/null; then
    echo "✅ Model files already exist in S3."
else
    echo "Uploading model files to S3..."
    if [ -d "../movie-engine-data/models" ]; then
        aws s3 sync ../movie-engine-data/models/ s3://movie-engine-data/models/
        echo "✅ Model files uploaded successfully."
    else
        echo "⚠️  Warning: Model files not found in ../movie-engine-data/models/"
        echo "You may need to upload them manually after deployment."
    fi
fi

# Check if processed data exists
echo "Checking processed data..."
if aws s3 ls s3://movie-engine-data/processed/ 2>&1 > /dev/null; then
    echo "✅ Processed data already exists in S3."
else
    echo "Uploading processed data to S3..."
    if [ -d "../movie-engine-data/processed" ]; then
        aws s3 sync ../movie-engine-data/processed/ s3://movie-engine-data/processed/
        echo "✅ Processed data uploaded successfully."
    else
        echo "⚠️  Warning: Processed data not found in ../movie-engine-data/processed/"
    fi
fi
echo ""

# Step 3: Deploy Frontend Infrastructure
echo "🌐 Step 3/4: Deploying Frontend Infrastructure..."
echo "========================================="
cdk deploy MovieEngineFrontendStack --require-approval never
echo ""

# Step 4: Build and Upload Frontend
echo "🚀 Step 4/4: Building and Uploading Frontend..."
echo "========================================="
./deploy_frontend.sh
echo ""

echo "========================================="
echo "   ✅ Deployment Complete!"
echo "========================================="
echo ""

# Get URLs
API_URL=$(aws cloudformation describe-stacks \
    --stack-name MovieEngineAPIStack \
    --query 'Stacks[0].Outputs[?ExportName==`MovieEngineApiUrl`].OutputValue' \
    --output text 2>/dev/null)

FRONTEND_URL=$(aws cloudformation describe-stacks \
    --stack-name MovieEngineFrontendStack \
    --query 'Stacks[0].Outputs[?ExportName==`MovieEngineFrontendUrl`].OutputValue' \
    --output text 2>/dev/null)

echo "🎬 Movie Engine URLs:"
echo "  Frontend: $FRONTEND_URL"
echo "  API:      $API_URL"
echo ""
echo "📝 Next Steps:"
echo "  1. Visit the frontend URL in your browser"
echo "  2. Start rating movies to get recommendations"
echo "  3. Monitor Lambda logs: aws logs tail /aws/lambda/movie-recommendation-api --follow"
echo ""
