# Movie Engine Infrastructure

AWS CDK infrastructure for deploying the Movie Recommendation System, including both the backend API and frontend web application.

## Overview

This directory contains AWS CDK stacks and deployment scripts for a complete movie recommendation system:

- **Backend API**: Lambda-based FastAPI application with API Gateway
- **Frontend Web App**: React SPA hosted on S3
- **Data Storage**: S3 bucket for ML models and datasets

## Architecture

```
┌─────────────────┐
│   React SPA     │
│  (S3 Bucket)    │
└────────┬────────┘
         │ HTTP
         ▼
┌─────────────────┐
│  API Gateway    │
│   (HTTP API)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌─────────────────┐
│  Lambda         │─────▶│  S3 Bucket      │
│  FastAPI App    │      │  Model Files    │
└─────────────────┘      └─────────────────┘
```

## Files

- **`app.py`**: Main CDK application that synthesizes both stacks
- **`movie_engine_api_stack.py`**: Backend API infrastructure (Lambda, API Gateway, S3)
- **`movie_engine_frontend_stack_simple.py`**: Frontend infrastructure (S3 website hosting)
- **`movie_engine_frontend_stack.py`**: Alternative frontend with CloudFront (optional)
- **`deploy_frontend.sh`**: Automated frontend deployment script
- **`generate_config.sh`**: Generate runtime configuration file
- **`show_config.sh`**: Display current configuration
- **`requirements.txt`**: CDK Python dependencies
- **`cdk.json`**: CDK configuration

## Prerequisites

- **AWS CLI** configured with credentials
- **AWS CDK** v2 installed (`npm install -g aws-cdk`)
- **Python 3.8+** for CDK
- **Node.js 18+** for frontend build
- **AWS Account** with appropriate permissions
- **TMDB API Key** (optional but recommended) - Get one free at [themoviedb.org](https://www.themoviedb.org/settings/api)

## Quick Start - Full Deployment

Deploy both backend and frontend in one go:

```bash
# 1. Install CDK dependencies
cd infra
pip install -r requirements.txt

# 2. Bootstrap CDK (first time only)
cdk bootstrap

# 3. Set TMDB API key (optional but recommended for movie posters/overviews)
export TMDB_API_KEY="your_api_key_here"

# 4. Deploy backend API
cdk deploy MovieEngineAPIStack

# 5. Upload model files to S3
aws s3 sync ../movie-engine-data/models s3://movie-engine-data/models/
aws s3 sync ../movie-engine-data/processed s3://movie-engine-data/processed/

# 6. Deploy frontend (automatically configures API URL)
./deploy_frontend.sh
```

Your application is now live! The deployment script will output:
- **API URL**: Use for testing the backend
- **Website URL**: Access your frontend application

## Deployment Guides

### Backend API Deployment

For detailed backend deployment instructions, including:
- Model file upload procedures
- Lambda configuration options
- API testing and monitoring
- Troubleshooting

See **[BACKEND_DEPLOYMENT.md](./BACKEND_DEPLOYMENT.md)**

### Frontend Deployment

For detailed frontend deployment instructions, including:
- API URL configuration methods
- Alternative deployment options
- CloudFront setup (optional)
- SPA routing configuration

See **[FRONTEND_DEPLOYMENT.md](./FRONTEND_DEPLOYMENT.md)**

## Deployment Options

### Option 1: Automated Deployment (Recommended)

Use the provided script for seamless deployment:

```bash
# Deploy backend first
cdk deploy MovieEngineAPIStack

# Deploy frontend with automatic API URL configuration
./deploy_frontend.sh
```

### Option 2: Independent Stack Deployment

Deploy stacks independently for more control:

```bash
# Backend only
cdk deploy MovieEngineAPIStack

# Frontend only
cdk deploy MovieEngineFrontendStack

# Then manually build and sync frontend
cd ../movie-engine-fe
npm run build
aws s3 sync dist/ s3://movie-engine-frontend/ --delete
```

### Option 3: Deploy All Stacks Together

Deploy everything at once:

```bash
cdk deploy --all
```

Note: You'll still need to upload model files and build the frontend separately.

## Configuration Management

### TMDB API Key

The backend uses The Movie Database (TMDB) API to fetch movie posters and plot overviews. While optional, it's highly recommended for the best user experience.

**Get a free API key:**
1. Sign up at [themoviedb.org](https://www.themoviedb.org/)
2. Go to Settings → API → Request API Key (Developer)
3. Copy your API key (v3 auth)

**Configure for deployment:**

```bash
# Set as environment variable before deploying
export TMDB_API_KEY="your_api_key_here"
cdk deploy MovieEngineAPIStack
```

**Update existing Lambda function:**

```bash
# Add to existing Lambda
aws lambda update-function-configuration \
    --function-name movie-recommendation-api \
    --environment "Variables={TMDB_API_KEY=your_api_key_here}"
```

Or via AWS Console:
1. Open Lambda → movie-recommendation-api
2. Configuration → Environment variables
3. Add: `TMDB_API_KEY` = `your_api_key_here`

**Without TMDB API key:** The app still works but shows placeholder images instead of movie posters and no plot descriptions.

### API URL Configuration

The frontend needs the API Gateway URL. The deployment script handles this automatically:

```bash
./deploy_frontend.sh
```

**Manual configuration** (if needed):
```bash
# Get API URL from CloudFormation
API_URL=$(aws cloudformation describe-stacks \
    --stack-name MovieEngineAPIStack \
    --query "Stacks[0].Outputs[?ExportName=='MovieEngineApiUrl'].OutputValue" \
    --output text)

# Set for frontend build
cd ../movie-engine-fe
echo "VITE_API_URL=$API_URL" > .env
npm run build
```

### Environment Variables

Check current configuration:
```bash
./show_config.sh
```

Generate runtime config file:
```bash
./generate_config.sh
```

## Testing Your Deployment

### Test Backend API

```bash
# Get API URL
API_URL=$(aws cloudformation describe-stacks \
    --stack-name MovieEngineAPIStack \
    --query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' \
    --output text)

# Health check
curl $API_URL/health

# Get recommendations
curl -X POST $API_URL/recommendations \
    -H "Content-Type: application/json" \
    -d '{"user_ratings": {"1": 5.0, "50": 4.5}, "n": 5}'
```

### Test Frontend

```bash
# Get website URL
WEBSITE_URL=$(aws cloudformation describe-stacks \
    --stack-name MovieEngineFrontendStack \
    --query 'Stacks[0].Outputs[?OutputKey==`WebsiteUrl`].OutputValue' \
    --output text)

# Open in browser
open $WEBSITE_URL
```

## Monitoring

### CloudWatch Logs

```bash
# Stream API Lambda logs
aws logs tail /aws/lambda/movie-recommendation-api --follow

# View API Gateway logs
aws logs tail "API-Gateway-Execution-Logs_<api-id>/<stage>" --follow
```

### CloudWatch Metrics

Monitor key metrics:
- **Lambda Duration**: Function execution time
- **Lambda Errors**: Failed invocations
- **API Gateway 4XX/5XX**: HTTP errors
- **API Gateway Latency**: Request response times

## Common CDK Commands

```bash
# List all stacks
cdk ls

# Show CloudFormation template
cdk synth

# Compare deployed vs current state
cdk diff

# Deploy specific stack
cdk deploy MovieEngineAPIStack

# Deploy all stacks
cdk deploy --all

# Destroy stack (keeps S3 data)
cdk destroy MovieEngineAPIStack
```

## Update Deployments

### Update Backend

```bash
# Make changes to Lambda code or stack configuration
# Then redeploy
cdk deploy MovieEngineAPIStack
```

### Update Frontend

```bash
# Make changes to React code
# Then rebuild and sync
./deploy_frontend.sh
```

Or manually:
```bash
cd ../movie-engine-fe
npm run build
aws s3 sync dist/ s3://movie-engine-frontend/ --delete \
    --cache-control "public, max-age=31536000, immutable" \
    --exclude "index.html"
aws s3 cp dist/index.html s3://movie-engine-frontend/index.html \
    --cache-control "no-cache"
```

### Update Model Files

```bash
# Sync new models to S3
aws s3 sync ../movie-engine-data/models s3://movie-engine-data/models/

# Lambda will automatically use new files on next invocation
```

## Cost Estimates

Typical monthly costs (low traffic):
- **Lambda**: ~$0.20 (100 requests/day)
- **API Gateway**: ~$1.00 (100 requests/day)
- **S3 Storage**: ~$0.02 (1GB models + frontend)
- **S3 Requests**: ~$0.01
- **CloudWatch Logs**: ~$0.50

**Total: ~$2/month** for development/testing

Production costs scale with usage. Some easy optimizations if this were taken further:
- Lambda provisioned concurrency (~$35/month per instance)
- CloudFront distribution (~$1/month + data transfer)
- API Gateway caching (~$0.02/hour)

## Security

- **S3 Data Bucket**: Private, Lambda access only
- **S3 Frontend Bucket**: Public read for website hosting
- **Lambda**: Minimal IAM permissions (S3 read only)
- **API Gateway**: HTTPS only, CORS enabled

## Troubleshooting

### Missing movie posters or descriptions
Set the TMDB_API_KEY environment variable (see Configuration Management section above)

### Lambda timeout errors
Increase timeout in `movie_engine_api_stack.py` (default 60s)

### Out of memory errors
Increase Lambda memory in `movie_engine_api_stack.py` (default 3GB)

### Frontend shows wrong API URL
Rebuild and redeploy frontend:
```bash
./deploy_frontend.sh
```

### Model files not found
Verify files exist in S3:
```bash
aws s3 ls s3://movie-engine-data/models/
```

### Stack deployment fails
Check CloudFormation events:
```bash
aws cloudformation describe-stack-events --stack-name MovieEngineAPIStack
```

## Cleanup

Remove all infrastructure:

```bash
# Destroy stacks
cdk destroy --all

# Manually delete S3 buckets (CDK retains by default)
aws s3 rm s3://movie-engine-data --recursive
aws s3 rb s3://movie-engine-data
aws s3 rm s3://movie-engine-frontend --recursive
aws s3 rb s3://movie-engine-frontend
```

## Production Considerations

Were this to be a real app, the following would be done:

1. **Custom Domain**: Configure Route 53 and ACM certificates
2. **Enable CloudFront**: Use `movie_engine_frontend_stack.py` for CDN
3. **Set up Monitoring**: Add CloudWatch alarms and dashboards
4. **Implement CI/CD**: Automate deployments with GitHub Actions or CodePipeline
5. **Enable WAF**: Protect API Gateway from common attacks
6. **Configure Backup**: Enable S3 versioning for model files
7. **Optimize Costs**: Review Lambda memory/timeout settings

## Support

For detailed deployment information:
- Backend API: See [BACKEND_DEPLOYMENT.md](./BACKEND_DEPLOYMENT.md)
- Frontend: See [FRONTEND_DEPLOYMENT.md](./FRONTEND_DEPLOYMENT.md)
- CDK Issues: Check CloudFormation events and CDK documentation
