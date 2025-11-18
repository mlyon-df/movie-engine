# Frontend Deployment Guide

This directory contains the CDK stack for deploying the Movie Engine frontend to AWS S3.

## Infrastructure

The `MovieEngineFrontendStack` creates:
- **S3 Bucket**: `movie-engine-frontend` configured for static website hosting
- **Public Access**: Enabled for serving the website
- **SPA Routing**: index.html as error document for client-side routing

## API URL Configuration

The frontend needs to know the API Gateway URL to make requests. There are three ways to configure this:

### Method 1: Automatic Configuration (Recommended)

The deployment script automatically fetches the API URL from CloudFormation and injects it during build:

```bash
./deploy_frontend.sh
```

This script:
1. Fetches the API URL from the `MovieEngineAPIStack` CloudFormation outputs
2. Creates a `.env` file with `VITE_API_URL=<api-url>`
3. Builds the frontend with the correct API URL baked in
4. Deploys to S3

### Method 2: Manual Environment Variable

Set the API URL before building:

```bash
# Get the API URL
API_URL=$(aws cloudformation describe-stacks \
    --stack-name MovieEngineAPIStack \
    --query "Stacks[0].Outputs[?ExportName=='MovieEngineApiUrl'].OutputValue" \
    --output text)

# Create .env file
cd ../movie-engine-fe
echo "VITE_API_URL=$API_URL" > .env

# Build
npm run build
```

### Method 3: Runtime Configuration (Advanced)

For runtime configuration (useful for multiple environments), generate a config file:

```bash
./generate_config.sh
```

Then update `index.html` to load the config and modify `api.js` to read from `window.APP_CONFIG`.

## Deployment Options

### Option 1: CDK Deployment (Recommended)

Deploy the entire stack including the S3 bucket:

```bash
# From the infra directory
cdk deploy MovieEngineFrontendStack
```

This will:
1. Create the S3 bucket with website hosting enabled
2. Output the website URL

After deployment, you'll need to build and upload the frontend manually:

```bash
# Build the frontend
cd ../movie-engine-fe
npm run build

# Upload to S3
aws s3 sync dist/ s3://movie-engine-frontend/ --delete
```

### Option 2: Script Deployment (Recommended)

Use the provided deployment script which automatically configures the API URL:

```bash
# Make script executable
chmod +x deploy_frontend.sh

# Deploy (will fetch API URL automatically)
./deploy_frontend.sh
```

This script will:
1. Fetch the API URL from CloudFormation
2. Build the frontend with the correct API URL
3. Sync files to S3 with appropriate cache headers
4. Upload index.html with no-cache headers for SPA updates

### Option 3: Manual Deployment

```bash
# 1. Build the frontend
cd ../movie-engine-fe
npm run build

# 2. Create bucket (if not exists)
aws s3 mb s3://movie-engine-frontend

# 3. Configure bucket for website hosting
aws s3 website s3://movie-engine-frontend/ \
    --index-document index.html \
    --error-document index.html

# 4. Make bucket publicly readable
aws s3api put-bucket-policy \
    --bucket movie-engine-frontend \
    --policy '{
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::movie-engine-frontend/*"
        }]
    }'

# 5. Upload files
aws s3 sync dist/ s3://movie-engine-frontend/ --delete
```

## Access the Website

After deployment, access your website at:
- **S3 Website URL**: `http://movie-engine-frontend.s3-website-us-east-1.amazonaws.com`

(Replace `us-east-1` with your region if different)

## Configure API URL

The deployment script (`deploy_frontend.sh`) automatically configures the API URL by:
1. Querying CloudFormation for the `MovieEngineApiUrl` export
2. Creating a `.env` file with `VITE_API_URL`
3. Building the frontend with this environment variable

**Manual configuration** (if needed):
```bash
# Get API URL from CloudFormation
API_URL=$(aws cloudformation describe-stacks \
    --stack-name MovieEngineAPIStack \
    --query "Stacks[0].Outputs[?ExportName=='MovieEngineApiUrl'].OutputValue" \
    --output text)

# In movie-engine-fe directory
echo "VITE_API_URL=$API_URL" > .env
npm run build
```

## Cleanup

To remove the frontend infrastructure:

```bash
cdk destroy MovieEngineFrontendStack
```

This will delete the bucket and all its contents.

## Alternative: CloudFront Distribution

For production deployments with HTTPS, consider using the full stack in `movie_engine_frontend_stack.py` which includes:
- CloudFront distribution
- HTTPS support
- Better caching and performance
- Custom domain support (requires additional configuration)

To use the CloudFront version, update `app.py` to import `movie_engine_frontend_stack` instead of `movie_engine_frontend_stack_simple`.
