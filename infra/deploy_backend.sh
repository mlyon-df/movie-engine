#!/bin/bash
# Deploy backend API to AWS Lambda
# Usage: ./deploy_backend.sh

set -e

echo "=== Movie Engine Backend Deployment ==="
echo ""

# Check if TMDB API key is provided as environment variable
if [ -z "$TMDB_API_KEY" ]; then
    echo "TMDB API key not found in environment."
    echo "Please enter your TMDB API key (from https://www.themoviedb.org/settings/api):"
    read -s TMDB_API_KEY
    echo ""
    
    if [ -z "$TMDB_API_KEY" ]; then
        echo "Error: TMDB API key is required."
        exit 1
    fi
fi

echo "TMDB API key provided."
echo ""

# Upload model files to S3 (if they haven't been uploaded yet)
echo "Checking if model files need to be uploaded to S3..."
BUCKET_NAME="movie-engine-data"
REGION="us-west-2"

# Check if bucket exists and has model files
if aws s3 ls s3://${BUCKET_NAME}/models/ --region ${REGION} 2>&1 > /dev/null; then
    echo "Model files already exist in S3."
else
    echo "Uploading model files to S3..."
    if [ -d "../movie-engine-data/models" ]; then
        aws s3 sync ../movie-engine-data/models/ s3://${BUCKET_NAME}/models/ --region ${REGION}
        echo "Model files uploaded successfully."
    else
        echo "Warning: Model files not found in ../movie-engine-data/models/"
        echo "You may need to upload them manually after deployment."
    fi
fi
echo ""

# Upload processed data (movies.csv, ratings.csv)
echo "Checking if processed data needs to be uploaded..."
if aws s3 ls s3://${BUCKET_NAME}/processed/ --region ${REGION} 2>&1 > /dev/null; then
    echo "Processed data already exists in S3."
else
    echo "Uploading processed data to S3..."
    if [ -d "../movie-engine-data/processed" ]; then
        aws s3 sync ../movie-engine-data/processed/ s3://${BUCKET_NAME}/processed/ --region ${REGION}
        echo "Processed data uploaded successfully."
    else
        echo "Warning: Processed data not found in ../movie-engine-data/processed/"
    fi
fi
echo ""

# Deploy CDK stack with TMDB API key
echo "Deploying CDK stack..."
export TMDB_API_KEY
cdk deploy MovieEngineAPIStack --require-approval never

echo ""
echo "=== Backend Deployment Complete ==="
echo ""
echo "Get your API URL with:"
echo "  aws cloudformation describe-stacks --stack-name MovieEngineAPIStack --query 'Stacks[0].Outputs[?ExportName==\`MovieEngineApiUrl\`].OutputValue' --output text"
echo ""